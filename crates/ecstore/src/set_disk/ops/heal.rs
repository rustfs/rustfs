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
use crate::disk::disk_store::DiskStoreRenameDataExt;
use crate::io_support::bitrot::object_mmap_read_enabled;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind, trace_emit};
use tracing::trace;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_HEAL_OBJECT_RENAME: &str = "heal_object_rename";
const HEAL_RENAME_INCOMPLETE: &str = "heal rename incomplete";

#[cfg(test)]
static HEAL_RENAME_FAILURES: std::sync::Mutex<Vec<(String, String, usize)>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
struct HealRenameFailureScope {
    bucket: String,
    object: String,
}

#[cfg(test)]
impl HealRenameFailureScope {
    fn install(bucket: &str, object: &str, disk_indexes: &[usize]) -> Self {
        let mut failures = HEAL_RENAME_FAILURES
            .lock()
            .expect("heal rename failure registry should not poison");
        assert!(
            !failures
                .iter()
                .any(|(registered_bucket, registered_object, _)| { registered_bucket == bucket && registered_object == object }),
            "heal rename failures must be installed once per object"
        );
        failures.extend(
            disk_indexes
                .iter()
                .map(|index| (bucket.to_string(), object.to_string(), *index)),
        );
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
        }
    }
}

#[cfg(test)]
impl Drop for HealRenameFailureScope {
    fn drop(&mut self) {
        HEAL_RENAME_FAILURES
            .lock()
            .expect("heal rename failure registry should not poison")
            .retain(|(bucket, object, _)| bucket != &self.bucket || object != &self.object);
    }
}

#[cfg(test)]
fn should_fail_heal_rename(bucket: &str, object: &str, disk_index: usize) -> bool {
    let mut failures = HEAL_RENAME_FAILURES
        .lock()
        .expect("heal rename failure registry should not poison");
    if let Some(position) = failures
        .iter()
        .position(|(registered_bucket, registered_object, registered_index)| {
            registered_bucket == bucket && registered_object == object && *registered_index == disk_index
        })
    {
        failures.swap_remove(position);
        true
    } else {
        false
    }
}

#[cfg(not(test))]
fn should_fail_heal_rename(_bucket: &str, _object: &str, _disk_index: usize) -> bool {
    false
}

#[cfg(test)]
static HEAL_WRITER_FAILURES: std::sync::Mutex<Vec<(String, String, usize, DiskError)>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
struct HealWriterFailureScope {
    bucket: String,
    object: String,
}

#[cfg(test)]
impl HealWriterFailureScope {
    fn install(bucket: &str, object: &str, disk_indexes: &[usize], error: DiskError) -> Self {
        let mut failures = HEAL_WRITER_FAILURES
            .lock()
            .expect("heal writer failure registry should not poison");
        assert!(
            !failures.iter().any(|(registered_bucket, registered_object, _, _)| {
                registered_bucket == bucket && registered_object == object
            }),
            "heal writer failures must be installed once per object"
        );
        failures.extend(
            disk_indexes
                .iter()
                .map(|index| (bucket.to_string(), object.to_string(), *index, error.clone())),
        );
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
        }
    }
}

#[cfg(test)]
impl Drop for HealWriterFailureScope {
    fn drop(&mut self) {
        HEAL_WRITER_FAILURES
            .lock()
            .expect("heal writer failure registry should not poison")
            .retain(|(bucket, object, _, _)| bucket != &self.bucket || object != &self.object);
    }
}

#[cfg(test)]
fn injected_heal_writer_error(bucket: &str, object: &str, disk_index: usize) -> Option<DiskError> {
    let mut failures = HEAL_WRITER_FAILURES
        .lock()
        .expect("heal writer failure registry should not poison");
    failures
        .iter()
        .position(|(registered_bucket, registered_object, registered_index, _)| {
            registered_bucket == bucket && registered_object == object && *registered_index == disk_index
        })
        .map(|position| failures.swap_remove(position).3)
}

#[cfg(not(test))]
fn injected_heal_writer_error(_bucket: &str, _object: &str, _disk_index: usize) -> Option<DiskError> {
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartFailureSummary {
    part_number: usize,
    failed_shards: usize,
    bitrot_failure: bool,
}

#[derive(Clone)]
struct RecoverableMetaCandidate {
    identity: [u8; 32],
    file_info: FileInfo,
    data_count: usize,
    local_payload: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DanglingDeleteSafety {
    UnsafeToDelete,
    NoRecoverableCandidate,
}

#[cfg(test)]
struct DanglingCheckPartsFailure {
    key: DanglingCheckPartsFailureKey,
}

#[cfg(test)]
type DanglingCheckPartsFailureKey = (String, String, usize);

#[cfg(test)]
type DanglingCheckPartsFailures = HashMap<DanglingCheckPartsFailureKey, DiskError>;

#[cfg(test)]
fn dangling_check_parts_failures() -> &'static std::sync::Mutex<DanglingCheckPartsFailures> {
    static FAILURES: std::sync::OnceLock<std::sync::Mutex<DanglingCheckPartsFailures>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
impl DanglingCheckPartsFailure {
    fn install(bucket: &str, object: &str, disk_index: usize, error: DiskError) -> Self {
        let key = (bucket.to_string(), object.to_string(), disk_index);
        let previous = dangling_check_parts_failures()
            .lock()
            .expect("dangling check-parts failure registry should not poison")
            .insert(key.clone(), error);
        assert!(previous.is_none(), "dangling check-parts failure already installed");
        Self { key }
    }
}

#[cfg(test)]
impl Drop for DanglingCheckPartsFailure {
    fn drop(&mut self) {
        dangling_check_parts_failures()
            .lock()
            .expect("dangling check-parts failure registry should not poison")
            .remove(&self.key);
    }
}

#[cfg(test)]
fn injected_dangling_check_parts_error(bucket: &str, object: &str, disk_index: usize) -> Option<DiskError> {
    dangling_check_parts_failures()
        .lock()
        .expect("dangling check-parts failure registry should not poison")
        .get(&(bucket.to_string(), object.to_string(), disk_index))
        .cloned()
}

#[cfg(test)]
struct DanglingDeleteFailure {
    key: DanglingDeleteFailureKey,
}

#[cfg(test)]
type DanglingDeleteFailureKey = (String, String, usize);

#[cfg(test)]
type DanglingDeleteFailures = HashMap<DanglingDeleteFailureKey, DiskError>;

#[cfg(test)]
fn dangling_delete_failures() -> &'static std::sync::Mutex<DanglingDeleteFailures> {
    static FAILURES: std::sync::OnceLock<std::sync::Mutex<DanglingDeleteFailures>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
impl DanglingDeleteFailure {
    fn install(bucket: &str, object: &str, disk_index: usize, error: DiskError) -> Self {
        let key = (bucket.to_string(), object.to_string(), disk_index);
        let previous = dangling_delete_failures()
            .lock()
            .expect("dangling delete failure registry should not poison")
            .insert(key.clone(), error);
        assert!(previous.is_none(), "dangling delete failure already installed");
        Self { key }
    }
}

#[cfg(test)]
impl Drop for DanglingDeleteFailure {
    fn drop(&mut self) {
        dangling_delete_failures()
            .lock()
            .expect("dangling delete failure registry should not poison")
            .remove(&self.key);
    }
}

#[cfg(test)]
fn injected_dangling_delete_error(bucket: &str, object: &str, disk_index: usize) -> Option<DiskError> {
    dangling_delete_failures()
        .lock()
        .expect("dangling delete failure registry should not poison")
        .get(&(bucket.to_string(), object.to_string(), disk_index))
        .cloned()
}

#[cfg(not(test))]
fn injected_dangling_delete_error(_bucket: &str, _object: &str, _disk_index: usize) -> Option<DiskError> {
    None
}

fn first_unhealthy_part_summary(
    data_errs_by_part: &HashMap<usize, Vec<usize>>,
    parts: &[ObjectPartInfo],
) -> Option<PartFailureSummary> {
    data_errs_by_part
        .iter()
        .filter_map(|(part_index, part_errs)| {
            let failed_shards = count_part_not_success(part_errs);
            if failed_shards == 0 {
                return None;
            }
            Some((
                *part_index,
                PartFailureSummary {
                    part_number: parts.get(*part_index).map(|part| part.number).unwrap_or(part_index + 1),
                    failed_shards,
                    bitrot_failure: part_errs.contains(&CHECK_PART_FILE_CORRUPT),
                },
            ))
        })
        .min_by_key(|(part_index, _)| *part_index)
        .map(|(_, summary)| summary)
}

fn heal_writer_error_summary(error: &DiskError) -> String {
    match error {
        DiskError::Io(io_error) => format!("io::{:?}", io_error.kind()),
        _ => error.to_string(),
    }
}

fn warn_heal_writer_failures(
    bucket: &str,
    object: &str,
    version_id: &str,
    writer_failure_count: usize,
    result: &'static str,
    first_failure: &(usize, usize, String),
) {
    let (first_part_number, first_disk_index, first_error) = first_failure;
    warn!(
        event = EVENT_SET_DISK_HEAL,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_SET_DISK,
        bucket,
        object,
        version_id,
        writer_failure_count,
        first_part_number,
        first_disk_index,
        error = %first_error,
        result,
        state = "writer_unavailable",
        "Set disk object heal writer failures"
    );
}

impl SetDisks {
    /// Read back one healed version from every explicitly admitted replacement
    /// target. This is intentionally separate from the normal heal result: a
    /// successful result describes the transaction attempt, while automatic
    /// replacement completion needs physical evidence that survives a crash
    /// before its checkpoint is persisted.
    pub(crate) async fn replacement_targets_have_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        targets: &[String],
    ) -> disk::error::Result<bool> {
        let disks = self.get_disks_internal().await;
        let mut target_disks = Vec::with_capacity(targets.len());

        for target in targets {
            let Some(index) = self.set_endpoints.iter().position(|endpoint| endpoint.to_string() == *target) else {
                return Ok(false);
            };
            let Some(disk) = disks.get(index).and_then(Option::as_ref) else {
                return Ok(false);
            };
            target_disks.push(disk.clone());
        }

        let read_options = ReadOptions {
            incl_free_versions: false,
            read_data: true,
            healing: true,
        };
        let checks = target_disks.into_iter().map(|disk| {
            let task_read_options = read_options;
            async move {
                let file_info = match disk.read_version("", bucket, object, version_id, &task_read_options).await {
                    Ok(file_info) => file_info,
                    Err(
                        DiskError::DiskNotFound
                        | DiskError::VolumeNotFound
                        | DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound,
                    ) => return Ok(false),
                    Err(err) => return Err(err),
                };
                if !file_info_is_valid_for_metadata(&file_info) {
                    return Ok(false);
                }
                if !version_id.is_empty() && file_info.version_id.as_ref().map(ToString::to_string).as_deref() != Some(version_id)
                {
                    return Ok(false);
                }
                if file_info.is_canonical_delete_marker() || file_info.is_remote() {
                    return Ok(true);
                }
                if (file_info.data.is_some() || file_info.size == 0) && !file_info.parts.is_empty() {
                    return Ok(true);
                }

                let check = match disk.check_parts(bucket, object, &file_info).await {
                    Ok(check) => check,
                    Err(
                        DiskError::DiskNotFound
                        | DiskError::VolumeNotFound
                        | DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound,
                    ) => return Ok(false),
                    Err(err) => return Err(err),
                };
                Ok(!check.results.is_empty() && check.results.iter().all(|result| *result == CHECK_PART_SUCCESS))
            }
        });

        Ok(futures::future::try_join_all(checks)
            .await?
            .into_iter()
            .all(|committed| committed))
    }

    #[tracing::instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    pub(in crate::set_disk) async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> disk::error::Result<(HealResultItem, Option<DiskError>)> {
        Box::pin(self.heal_object_with_explicit_version_regen(bucket, object, version_id, opts, true)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn heal_object_with_explicit_version_regen(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
        allow_explicit_version_regen: bool,
    ) -> disk::error::Result<(HealResultItem, Option<DiskError>)> {
        trace!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            scan_mode = %opts.scan_mode.as_str(),
            dry_run = opts.dry_run,
            remove = opts.remove,
            state = "started",
            "Set disk object heal started"
        );

        let disks = self.get_disks_internal().await;

        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            disk_count: disks.len(),
            ..Default::default()
        };

        // Bound, not `_`: this guard must live to the end of the scope. A bare
        // `_` would drop it here and release the namespace write lock.
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

        let version_id_op = {
            if version_id.is_empty() {
                None
            } else {
                Some(version_id.to_string())
            }
        };

        let (mut parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, "", bucket, object, version_id, true, true, false).await?;

        trace!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            parts_count = parts_metadata.len(),
            error_count = errs.iter().flatten().count(),
            state = "metadata_read",
            "Set disk object metadata read"
        );
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

        trace!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            parts_count = parts_metadata.len(),
            state = "quorum_check",
            "Set disk object quorum check started"
        );
        match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count) {
            Ok((read_quorum, _)) => {
                result.parity_blocks = result.disk_count - read_quorum as usize;
                result.data_blocks = read_quorum as usize;

                let ((mut online_disks, quorum_mod_time, quorum_etag), disk_len) = {
                    let disks = self.disks.read().await;
                    let disk_len = disks.len();
                    (Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize), disk_len)
                };

                trace!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    metadata_count = parts_metadata.len(),
                    error_count = errs.iter().flatten().count(),
                    read_quorum,
                    disk_count = disk_len,
                    online_disk_count = online_disks.iter().flatten().count(),
                    state = "disk_metadata_resolved",
                    "Set disk object metadata resolved"
                );

                let filter_by_etag = quorum_etag.is_some();
                match Self::pick_valid_fileinfo(&parts_metadata, quorum_mod_time, quorum_etag.clone(), read_quorum as usize) {
                    Ok(mut latest_meta) => {
                        Self::hydrate_selected_fileinfo_part_checksums(&mut latest_meta)?;
                        trace!(
                            event = EVENT_SET_DISK_HEAL,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            deleted = latest_meta.deleted,
                            remote = latest_meta.is_remote(),
                            inline = latest_meta.inline_data(),
                            part_count = latest_meta.parts.len(),
                            data_shards = latest_meta.erasure.data_blocks,
                            parity_shards = latest_meta.erasure.parity_blocks,
                            state = "canonical_metadata_selected",
                            "Set disk canonical object metadata selected"
                        );

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

                        trace!(
                            event = EVENT_SET_DISK_HEAL,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            available_disk_count = online_disks.iter().flatten().count(),
                            disk_count = online_disks.len(),
                            state = "parts_checked",
                            "Set disk object parts checked"
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
                                debug!(
                                    event = EVENT_SET_DISK_HEAL,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                                    bucket,
                                    object,
                                    version_id,
                                    disk_index = index,
                                    endpoint = %self.set_endpoints[index],
                                    state = "disk_marked_for_healing",
                                    "Set disk marked for healing"
                                );
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

                        if cannot_heal {
                            let total_disks = parts_metadata.len();
                            let healthy_count = total_disks.saturating_sub(disks_to_heal_count);
                            let required_data = total_disks.saturating_sub(latest_meta.erasure.parity_blocks);
                            let no_parity_failure =
                                (!latest_meta.deleted && !latest_meta.is_remote() && latest_meta.erasure.parity_blocks == 0)
                                    .then(|| first_unhealthy_part_summary(&data_errs_by_part, &latest_meta.parts))
                                    .flatten();
                            let cannot_heal_err = if no_parity_failure.is_some_and(|failure| failure.bitrot_failure) {
                                DiskError::FileCorrupt
                            } else {
                                DiskError::ErasureReadQuorum
                            };

                            if let Some(failure) = no_parity_failure {
                                result.detail = format!(
                                    "no-parity object is unrecoverable: part {} has {} missing or corrupt data shard(s), bitrot_failure={}, data_blocks={}, parity_blocks=0",
                                    failure.part_number,
                                    failure.failed_shards,
                                    failure.bitrot_failure,
                                    latest_meta.erasure.data_blocks
                                );
                                error!(
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_HEAL,
                                    bucket,
                                    object,
                                    version_id,
                                    data_shards = latest_meta.erasure.data_blocks,
                                    parity_shards = latest_meta.erasure.parity_blocks,
                                    required_data_shards = required_data,
                                    healthy_shards = healthy_count,
                                    missing_or_corrupt_shards = disks_to_heal_count,
                                    part_number = failure.part_number,
                                    bitrot_failure = failure.bitrot_failure,
                                    "No-parity object failed integrity or availability validation and cannot be reconstructed"
                                );
                            } else {
                                result.detail = format!(
                                    "object cannot be reconstructed with available shards: required_data_shards={required_data}, healthy_shards={healthy_count}, missing_or_corrupt_shards={disks_to_heal_count}, parity_shards={}",
                                    latest_meta.erasure.parity_blocks
                                );
                                error!(
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_HEAL,
                                    bucket,
                                    object,
                                    version_id,
                                    required_data_shards = required_data,
                                    healthy_shards = healthy_count,
                                    missing_or_corrupt_shards = disks_to_heal_count,
                                    parity_shards = latest_meta.erasure.parity_blocks,
                                    "Heal object cannot reconstruct with available shards"
                                );
                            }

                            // `disks_with_all_parts` normalizes conflicting entries
                            // in `parts_metadata` to defaults. Re-read only before
                            // destructive cleanup so the guard sees every original
                            // identity.
                            let (delete_guard_metadata, delete_guard_errs) =
                                Self::read_all_fileinfo(&disks, "", bucket, object, version_id, true, true, false).await?;
                            if self
                                .dangling_delete_safety(bucket, object, &delete_guard_metadata, &delete_guard_errs, &disks)
                                .await?
                                == DanglingDeleteSafety::UnsafeToDelete
                            {
                                return Ok((result, Some(cannot_heal_err)));
                            }

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
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_HEAL,
                                        bucket,
                                        object,
                                        version_id,
                                        error = %err,
                                        returned_error = %cannot_heal_err,
                                        "Heal object dangling cleanup could not prove object deletion"
                                    );
                                    Ok((result, Some(cannot_heal_err)))
                                }
                            };
                        }

                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != online_disks.len() {
                            let distribution_len = latest_meta.erasure.distribution.len();
                            let disk_slot_count = online_disks.len();
                            let err_str = format!(
                                "unexpected file distribution length {distribution_len} for {disk_slot_count} disk slots; backend disks may have been manually modified; refusing to heal {bucket}/{object}({version_id})"
                            );
                            warn!(
                                event = EVENT_SET_DISK_HEAL,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                bucket,
                                object,
                                version_id,
                                distribution_len,
                                disk_slot_count,
                                state = "invalid_distribution",
                                "Set disk object heal refused due to invalid erasure distribution"
                            );
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        let latest_disks = Self::shuffle_disks(&online_disks, &latest_meta.erasure.distribution);
                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != out_dated_disks.len() {
                            let distribution_len = latest_meta.erasure.distribution.len();
                            let disk_slot_count = out_dated_disks.len();
                            let err_str = format!(
                                "unexpected file distribution length {distribution_len} for {disk_slot_count} disk slots; backend disks may have been manually modified; refusing to heal {bucket}/{object}({version_id})"
                            );
                            warn!(
                                event = EVENT_SET_DISK_HEAL,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                bucket,
                                object,
                                version_id,
                                distribution_len,
                                disk_slot_count,
                                state = "invalid_distribution",
                                "Set disk object heal refused due to invalid erasure distribution"
                            );
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != parts_metadata.len() {
                            let distribution_len = latest_meta.erasure.distribution.len();
                            let metadata_count = parts_metadata.len();
                            let err_str = format!(
                                "unexpected file distribution length {distribution_len} for {metadata_count} metadata entries; backend disks may have been manually modified; refusing to heal {bucket}/{object}({version_id})"
                            );
                            warn!(
                                event = EVENT_SET_DISK_HEAL,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                bucket,
                                object,
                                version_id,
                                distribution_len,
                                metadata_count,
                                state = "invalid_distribution",
                                "Set disk object heal refused due to invalid erasure distribution"
                            );
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
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_HEAL,
                                        bucket,
                                        object,
                                        version_id,
                                        "Heal object latest metadata has no data_dir, cannot heal object data"
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
                            let mut writer_failure_count = 0usize;
                            let mut first_writer_failure = None;
                            let mut writer_failure_warned = false;

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
                                        trace!(
                                            event = EVENT_SET_DISK_HEAL,
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                                            part_number = part.number,
                                            disk_index = index,
                                            part_status = this_part_errs[index],
                                            state = "source_shard_skipped",
                                            "Set disk source shard skipped"
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
                                            Err(_e) => {
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
                                        let writer_result = if let Some(error) = injected_heal_writer_error(bucket, object, index)
                                        {
                                            Err(error)
                                        } else {
                                            create_bitrot_writer(
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
                                        };
                                        let writer = match writer_result {
                                            Ok(writer) => writer,
                                            Err(err) => {
                                                writer_failure_count += 1;
                                                if first_writer_failure.is_none() {
                                                    first_writer_failure =
                                                        Some((part.number, index, heal_writer_error_summary(&err)));
                                                }
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
                                if writer_failure_count > 0
                                    && writers.iter().all(Option::is_none)
                                    && let Some(first_failure) = first_writer_failure.as_ref()
                                {
                                    warn_heal_writer_failures(
                                        bucket,
                                        object,
                                        version_id,
                                        writer_failure_count,
                                        "all_targets_unavailable",
                                        first_failure,
                                    );
                                    writer_failure_warned = true;
                                }
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
                                    if !writer_failure_warned && let Some(first_failure) = first_writer_failure.as_ref() {
                                        warn_heal_writer_failures(
                                            bucket,
                                            object,
                                            version_id,
                                            writer_failure_count,
                                            "all_targets_unavailable",
                                            first_failure,
                                        );
                                    }
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

                            if !writer_failure_warned && let Some(first_failure) = first_writer_failure.as_ref() {
                                warn_heal_writer_failures(
                                    bucket,
                                    object,
                                    version_id,
                                    writer_failure_count,
                                    "partial_targets_unavailable",
                                    first_failure,
                                );
                            }
                        }
                        // Rename from tmp location to the actual location.
                        // MinIO stops on the first RenameData error. RustFS intentionally
                        // continues per target, but reports any residue after all attempts
                        // so successful repairs survive and failed targets remain retryable.
                        let mut rename_attempts = 0usize;
                        let mut rename_successes = 0usize;
                        let mut healed_disks = vec![None; out_dated_disks.len()];
                        for (index, outdated_disk) in out_dated_disks.iter().enumerate() {
                            if let Some(disk) = outdated_disk {
                                rename_attempts += 1;
                                // record the index of the updated disks
                                parts_metadata[index].erasure.index = index + 1;
                                // Attempt a rename now from healed data to final location.
                                parts_metadata[index].set_healing();

                                let rename_result = if should_fail_heal_rename(bucket, object, index) {
                                    Err(DiskError::Unexpected)
                                } else {
                                    disk.rename_data_borrowed(
                                        RUSTFS_META_TMP_BUCKET,
                                        &tmp_id,
                                        &parts_metadata[index],
                                        bucket,
                                        object,
                                    )
                                    .await
                                };

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
                                    healed_disks[index] = Some(disk.clone());
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

                        self.record_healed_capacity_scope(&healed_disks);

                        if rename_successes < rename_attempts {
                            return Ok((
                                result,
                                Some(DiskError::other(format!(
                                    "{HEAL_RENAME_INCOMPLETE}: {rename_successes} of {rename_attempts} targets committed for \
                                     {bucket}/{object}"
                                ))),
                            ));
                        }

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
                if allow_explicit_version_regen
                    && !version_id.is_empty()
                    && self
                        .try_regenerate_explicit_version_meta(bucket, object, version_id, &parts_metadata, &errs, &disks)
                        .await?
                {
                    return Box::pin(self.heal_object_with_explicit_version_regen(bucket, object, version_id, opts, false)).await;
                }

                if self
                    .dangling_delete_safety(bucket, object, &parts_metadata, &errs, &disks)
                    .await?
                    == DanglingDeleteSafety::UnsafeToDelete
                {
                    return Ok((
                        self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                            .await,
                        Some(err),
                    ));
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

    async fn try_regenerate_explicit_version_meta(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<bool> {
        let Ok(version_id) = Uuid::parse_str(version_id) else {
            return Ok(false);
        };
        let candidates = parts_metadata
            .iter()
            .zip(errs.iter())
            .filter_map(|(file_info, err)| {
                (err.is_none()
                    && file_info_is_valid_for_metadata(file_info)
                    && file_info.version_id == Some(version_id)
                    && file_info.has_valid_erasure_geometry()
                    && !file_info.deleted
                    && !file_info.is_remote()
                    && file_info.data_dir.is_some()
                    && !file_info.parts.is_empty()
                    && file_info.erasure.data_blocks > 0
                    && file_info
                        .erasure
                        .data_blocks
                        .checked_add(file_info.erasure.parity_blocks)
                        .is_some_and(|shards| shards == disks.len()))
                .then_some(file_info)
            })
            .collect::<Vec<_>>();
        let Some(candidate) = candidates.first().copied() else {
            return Ok(false);
        };
        let identity = Self::file_info_quorum_hash(candidate);
        if candidates
            .iter()
            .any(|file_info| Self::file_info_quorum_hash(file_info) != identity)
        {
            return Ok(false);
        }

        let mut available = 0usize;
        for disk in disks {
            let Some(disk) = disk else {
                return Ok(false);
            };
            match disk.check_parts(bucket, object, candidate).await {
                Ok(response)
                    if !response.results.is_empty() && response.results.iter().all(|result| *result == CHECK_PART_SUCCESS) =>
                {
                    available += 1;
                }
                Ok(_)
                | Err(
                    DiskError::FileNotFound
                    | DiskError::FileVersionNotFound
                    | DiskError::PathNotFound
                    | DiskError::VolumeNotFound,
                ) => {}
                Err(_) => return Ok(false),
            }
        }
        if available < candidate.erasure.data_blocks {
            return Ok(false);
        }

        let mut wrote = 0usize;
        for (index, disk) in disks.iter().enumerate() {
            let Some(disk) = disk else {
                return Ok(false);
            };
            let metadata_absent = matches!(
                errs.get(index).and_then(Option::as_ref),
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)
            );
            if !metadata_absent {
                continue;
            }
            let Some(&shard_index) = candidate.erasure.distribution.get(index) else {
                return Ok(false);
            };
            let mut regenerated = candidate.clone();
            regenerated.fresh = false;
            regenerated.erasure.index = shard_index;
            match disk.write_metadata("", bucket, object, regenerated).await {
                Ok(()) => wrote += 1,
                Err(error) => {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_HEAL,
                        bucket,
                        object,
                        disk_index = index,
                        error = %error,
                        "failed to regenerate recoverable xl.meta"
                    );
                }
            }
        }
        Ok(wrote > 0)
    }

    /// Best-effort orphan-data-dir reclaim for an object that is healthy on this
    /// set. Wraps [`Self::reclaim_orphan_data_dirs`] with the shared logging so
    /// both `heal_object` exits — the already-healthy early return and the
    /// post-heal tail — reclaim identically. Never fails the heal: delete errors
    /// are logged and swallowed. Callers must gate this on `!opts.dry_run`.
    async fn reclaim_orphan_data_dirs_best_effort(&self, bucket: &str, object: &str) {
        match self.reconcile_old_data_cleanup_receipts(bucket, object).await {
            Ok(removed) if removed > 0 => {
                debug!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    removed,
                    state = "old_data_cleanup_receipt_reconciled",
                    "Set disk old-data cleanup receipts reconciled"
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    error = %e,
                    state = "old_data_cleanup_receipt_reconcile_failed",
                    "Set disk old-data cleanup receipt reconcile failed"
                );
            }
        }
        match self.reclaim_orphan_data_dirs(bucket, object).await {
            Ok(removed) if removed > 0 => {
                debug!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    removed,
                    state = "orphan_data_reclaimed",
                    "Set disk orphaned data reclaimed"
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    error = %e,
                    state = "orphan_data_reclaim_failed",
                    "Set disk orphan data-dir reclaim failed"
                );
            }
        }
    }

    /// Prevent dangling cleanup when surviving state cannot prove that deletion
    /// is safe. Part presence proves only recoverability, never commit: the write
    /// path can durably rename data before xl.meta is committed.
    async fn dangling_delete_safety(
        &self,
        bucket: &str,
        object: &str,
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<DanglingDeleteSafety> {
        if disks.iter().any(Option::is_none)
            || errs.iter().flatten().any(|err| {
                !matches!(
                    err,
                    DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound
                        | DiskError::VolumeNotFound
                )
            })
        {
            return Ok(DanglingDeleteSafety::UnsafeToDelete);
        }

        let mut candidates = Vec::<RecoverableMetaCandidate>::with_capacity(parts_metadata.len());
        for (fi, err) in parts_metadata.iter().zip(errs.iter()) {
            if err.is_some() || !file_info_is_valid_for_metadata(fi) {
                continue;
            }

            let identity = Self::file_info_quorum_hash(fi);
            if !candidates.iter().any(|candidate| candidate.identity == identity) {
                let local_payload = fi.has_valid_erasure_geometry()
                    && !fi.deleted
                    && !fi.is_remote()
                    && fi.data_dir.is_some()
                    && !fi.parts.is_empty()
                    && fi.erasure.data_blocks > 0
                    && fi
                        .erasure
                        .data_blocks
                        .checked_add(fi.erasure.parity_blocks)
                        .is_some_and(|shards| shards == disks.len());
                candidates.push(RecoverableMetaCandidate {
                    identity,
                    file_info: fi.clone(),
                    data_count: 0,
                    local_payload,
                });
            }
        }

        if candidates
            .iter()
            .any(|candidate| candidate.file_info.deleted || candidate.file_info.is_remote())
            || candidates.len() > 1
        {
            return Ok(DanglingDeleteSafety::UnsafeToDelete);
        }

        for candidate in candidates.iter_mut().filter(|candidate| candidate.local_payload) {
            for (disk_index, disk) in disks.iter().enumerate() {
                // Only the #[cfg(test)] fault-injection branch below reads this.
                #[cfg(not(test))]
                let _ = disk_index;
                let Some(disk) = disk else {
                    return Ok(DanglingDeleteSafety::UnsafeToDelete);
                };
                #[cfg(test)]
                let check_result = match injected_dangling_check_parts_error(bucket, object, disk_index) {
                    Some(error) => Err(error),
                    None => disk.check_parts(bucket, object, &candidate.file_info).await,
                };
                #[cfg(not(test))]
                let check_result = disk.check_parts(bucket, object, &candidate.file_info).await;

                match check_result {
                    Ok(resp) if !resp.results.is_empty() && resp.results.iter().all(|result| *result == CHECK_PART_SUCCESS) => {
                        candidate.data_count += 1;
                    }
                    Ok(_) => {}
                    Err(
                        DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound
                        | DiskError::VolumeNotFound,
                    ) => {}
                    Err(_) => return Ok(DanglingDeleteSafety::UnsafeToDelete),
                }
            }
        }

        Ok(
            if candidates
                .iter()
                .any(|candidate| candidate.local_payload && candidate.data_count >= candidate.file_info.erasure.data_blocks)
            {
                DanglingDeleteSafety::UnsafeToDelete
            } else {
                DanglingDeleteSafety::NoRecoverableCandidate
            },
        )
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
        let delete_errs = if dangling_object && !dry_run && remove {
            let mut futures = Vec::with_capacity(disks.len());
            for (disk_index, disk) in disks.iter().enumerate() {
                let disk = disk.clone();
                futures.push(async move {
                    let Some(disk) = disk else {
                        return (disk_index, Some(DiskError::DiskNotFound));
                    };
                    if let Some(error) = injected_dangling_delete_error(bucket, object, disk_index) {
                        return (disk_index, Some(error));
                    }
                    (
                        disk_index,
                        disk.delete(
                            bucket,
                            object,
                            DeleteOptions {
                                recursive: false,
                                immediate: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .err(),
                    )
                });
            }
            Some(join_all(futures).await)
        } else {
            None
        };

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

        if let Some(delete_errs) = delete_errs {
            let mut delete_failure = None;
            for (index, err) in delete_errs {
                match err {
                    None | Some(DiskError::FileNotFound) => {
                        result.after.drives[index].state = DriveState::Missing.to_string();
                    }
                    Some(err) => {
                        result.after.drives[index].state = if matches!(&err, DiskError::DiskNotFound) {
                            DriveState::Offline.to_string()
                        } else {
                            DriveState::Corrupt.to_string()
                        };
                        if delete_failure.is_none() {
                            delete_failure = Some(err);
                        }
                    }
                }
            }
            return Ok((result, Some(delete_failure.unwrap_or(DiskError::FileNotFound))));
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

    #[allow(
        dead_code,
        reason = "lock-taking wrapper over the live heal_object_dir_locked; only comments reference it (backlog#1823)"
    )]
    #[tracing::instrument(level = "trace", skip(self), fields(bucket = %bucket, object = %object))]
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

impl SetDisks {
    pub(crate) async fn heal_replacement_format(
        &self,
        dry_run: bool,
        targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        if targets.is_empty() {
            return Err(Error::other("replacement format requires at least one target"));
        }

        let mut target_slots = Vec::with_capacity(targets.len());
        for target in targets {
            let Some(slot) = self.set_endpoints.iter().position(|endpoint| endpoint.to_string() == *target) else {
                return Err(Error::other("replacement format target does not belong to the set"));
            };
            if target_slots.contains(&slot) {
                return Err(Error::other("replacement format target is duplicated"));
            }
            target_slots.push(slot);
        }

        self.heal_format_for_slots(dry_run, Some(&target_slots)).await
    }

    async fn heal_format_for_slots(
        &self,
        dry_run: bool,
        target_slots: Option<&[usize]>,
    ) -> Result<(HealResultItem, Option<Error>)> {
        let disks = self.disks.read().await.clone();
        let (formats, errs) = load_format_erasure_all(&disks, true).await;
        if errs.iter().any(|err| {
            matches!(
                err,
                Some(DiskError::InconsistentDisk | DiskError::CorruptedFormat | DiskError::CorruptedBackend)
            )
        }) {
            return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat)));
        }
        let slot_offset = self
            .set_index
            .checked_mul(self.set_drive_count)
            .ok_or_else(|| Error::other("erasure set slot offset overflow"))?;
        let ref_format = match get_format_erasure_in_quorum(&formats, slot_offset) {
            Ok(format) if format.shared_identity() == self.format.shared_identity() => format,
            Ok(_) => return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat))),
            Err(err) => {
                let can_use_cached_layout = count_errs(&errs, &DiskError::UnformattedDisk) > 0
                    && formats_match_reference_slots(&formats, &self.format, slot_offset)
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
        if !formats_match_reference_slots(&formats, &ref_format, slot_offset) {
            return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat)));
        }

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
            debug!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_HEAL,
                error_count = errs.iter().flatten().count(),
                result = "no_heal_required",
                "set disk formats success"
            );
            return Ok((result, Some(StorageError::NoHealRequired)));
        }

        if !dry_run {
            for (disk_idx, err) in errs.iter().enumerate() {
                if !matches!(err, Some(DiskError::UnformattedDisk))
                    || target_slots.is_some_and(|slots| !slots.contains(&disk_idx))
                {
                    continue;
                }

                let mut new_format = ref_format.clone();
                new_format.erasure.this = ref_format.erasure.sets[self.set_index][disk_idx];
                match save_format_file(&disks[disk_idx], &Some(new_format.clone())).await {
                    Ok(()) => {
                        result.after.drives[disk_idx].uuid = new_format.erasure.this.to_string();
                        result.after.drives[disk_idx].state = DriveState::Ok.to_string();
                    }
                    Err(err) => return Ok((result, Some(err.into()))),
                }
            }
        }

        Ok((result, None))
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
        self.heal_format_for_slots(dry_run, None).await
    }

    #[tracing::instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let mut result = heal_bucket_local_on_disks(bucket, opts, self.disk_inventory().await).await?;
        result.set_count = 1;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
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

    #[tracing::instrument(level = "debug", skip(self, opts), fields(bucket = %bucket, object = %object, dry_run = opts.dry_run))]
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        let started_at = std::time::Instant::now();
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

        let removed = if opts.dry_run {
            self.dry_run_reclaim_orphan_data_dirs(bucket, object).await?
        } else {
            self.reclaim_orphan_data_dirs(bucket, object).await?
        };
        let state = if opts.dry_run && removed > 0 {
            "dry_run_matched"
        } else if removed > 0 {
            "reclaimed"
        } else {
            "checked"
        };
        let data_dirs = u64::try_from(removed).unwrap_or(u64::MAX);

        trace_emit(|| {
            TraceEvent::new(TraceKind::Heal, TraceFunc::HealCheckAbandonedParts)
                .with_bucket(bucket)
                .with_object(object)
                .with_duration(started_at.elapsed())
                .with_attr("state", state)
                .with_attr("dry_run", opts.dry_run)
                .with_attr("data_dirs", data_dirs)
        });

        if removed > 0 {
            trace!(
                event = "heal_abandoned_parts",
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_HEAL,
                state = if opts.dry_run { "dry_run_matched" } else { "reclaimed" },
                result = "ok",
                bucket,
                object,
                dry_run = opts.dry_run,
                data_dirs = removed,
                "Heal abandoned parts checked object data directories"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod heal_result_report_tests {
    use super::{DanglingCheckPartsFailure, DanglingDeleteFailure, DanglingDeleteSafety, SetDisks, heal_writer_error_summary};
    use super::{HEAL_RENAME_INCOMPLETE, HealRenameFailureScope, HealWriterFailureScope};
    use crate::disk::endpoint::Endpoint;
    use crate::disk::error::DiskError;
    use crate::disk::format::FormatV3;
    use crate::disk::{DiskAPI as _, DiskOption, DiskStore, RUSTFS_META_TMP_BUCKET, ReadOptions, new_disk};
    use crate::error::Error;
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::set_disk::ops::object::hermetic_set_disks_support::hermetic_set_disks_isolated;
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::storage_api_contracts::heal::HealOperations as _;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use crate::{
        config::storageclass,
        store::init_format::{load_format_erasure, save_format_file},
    };
    use rustfs_common::heal_channel::{DriveState, HealOpts, HealScanMode};
    use rustfs_filemeta::{BLOCK_SIZE_V2, FileInfo, ObjectPartInfo, TRANSITION_COMPLETE};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use time::OffsetDateTime;
    use tokio::sync::RwLock;
    use tracing_subscriber::fmt::MakeWriter;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    #[test]
    fn heal_writer_error_summary_redacts_io_message() {
        let error = DiskError::Io(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "/sensitive/storage/path"));

        let summary = heal_writer_error_summary(&error);

        assert_eq!(summary, "io::PermissionDenied");
        assert!(!summary.contains("sensitive"));
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn heal_writer_failures_emit_one_aggregate_warning_per_object() {
        for (case, failed_target_count, expected_result, expect_error) in [
            ("partial", 1usize, "partial_targets_unavailable", false),
            ("all", 2usize, "all_targets_unavailable", true),
        ] {
            let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = format!("heal-writer-{case}");
            let object = "object.bin";
            for disk in &disks {
                disk.make_volume(&bucket).await.expect("bucket volume should be created");
            }

            let mut reader = PutObjReader::from_vec(vec![0x5a; 1024 * 1024]);
            set.put_object(&bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("source object should be written");
            let source = disks[2]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("source metadata should be readable");
            let data_dir = source.data_dir.expect("non-inline source should have a data directory");
            let mut target_slots = [source.erasure.distribution[0] - 1, source.erasure.distribution[1] - 1];
            target_slots.sort_unstable();

            for index in [0, 1] {
                tokio::fs::remove_file(
                    temp_dirs[index]
                        .path()
                        .join(&bucket)
                        .join(object)
                        .join(data_dir.to_string())
                        .join("part.1"),
                )
                .await
                .expect("target shard should be removed before heal");
            }

            let failed_slots = &target_slots[..failed_target_count];
            let logs = CapturedLogs::default();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_writer(logs.clone())
                .with_ansi(false)
                .without_time()
                .finish();
            let subscriber_guard = tracing::subscriber::set_default(subscriber);
            let failure_scope = HealWriterFailureScope::install(&bucket, object, failed_slots, DiskError::DiskFull);

            let heal_outcome = set
                .heal_object(
                    &bucket,
                    object,
                    "",
                    &HealOpts {
                        no_lock: true,
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await;
            drop(failure_scope);
            drop(subscriber_guard);

            assert_eq!(
                heal_outcome.is_err(),
                expect_error,
                "{case}: aggregate heal result should match writer outcomes"
            );
            let output = logs.contents();
            assert_eq!(
                output.matches("Set disk object heal writer failures").count(),
                1,
                "{case}: writer failures must emit one aggregate warning per object: {output}"
            );
            assert!(
                output.contains(&format!("writer_failure_count={failed_target_count}")),
                "{case}: warning must report the aggregate failure count: {output}"
            );
            assert!(
                output.contains(&format!("first_disk_index={}", failed_slots[0])),
                "{case}: warning must report the first failed target: {output}"
            );
            assert!(
                output.contains("first_part_number=1"),
                "{case}: warning must report the first failed part: {output}"
            );
            assert!(
                output.contains(&format!("result=\"{expected_result}\"")),
                "{case}: warning must distinguish partial from all-target failure: {output}"
            );
            assert!(
                output.contains("error=drive path full"),
                "{case}: warning must preserve a redacted failure reason: {output}"
            );
        }
    }

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

    fn meta_regen_test_fileinfo(object: &str, data_dir: Uuid, mod_time: i64, disk_index: usize) -> FileInfo {
        let mut fi = FileInfo::new(object, 2, 2);
        fi.data_dir = Some(data_dir);
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(mod_time).expect("test timestamp should parse"));
        fi.size = 1;
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        fi.erasure.index = fi.erasure.distribution[disk_index];
        fi
    }

    async fn meta_regen_test_set(
        bucket: &str,
        object: &str,
        data_dirs: &[(Uuid, usize)],
    ) -> (Vec<TempDir>, Arc<SetDisks>, Vec<Option<DiskStore>>) {
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for disk_index in 0..4 {
            let (temp_dir, endpoint, disk) = real_disk().await;
            disk.make_volume(bucket).await.expect("test bucket should be created");
            for (data_dir, shard_count) in data_dirs {
                if disk_index >= *shard_count {
                    continue;
                }
                let part_dir = temp_dir.path().join(bucket).join(object).join(data_dir.to_string());
                tokio::fs::create_dir_all(&part_dir)
                    .await
                    .expect("test data directory should be created");
                tokio::fs::write(part_dir.join("part.1"), [1u8; 2])
                    .await
                    .expect("test data shard should be written");
            }
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set = set_disks_with(disks.clone(), endpoints, 2).await;
        (temp_dirs, set, disks)
    }

    async fn seed_meta_regen_test_metadata(
        disks: &[Option<DiskStore>],
        disk_index: usize,
        bucket: &str,
        object: &str,
        file_info: &FileInfo,
    ) {
        disks[disk_index]
            .as_ref()
            .expect("metadata test disk should be online")
            .write_metadata("", bucket, object, file_info.clone())
            .await
            .expect("test metadata should be written");
    }

    async fn formatted_single_disk_no_parity_set() -> (TempDir, Arc<SetDisks>) {
        let format = FormatV3::new(1, 1);
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);

        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][0];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        let set = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![Some(disk)])),
            1,
            0,
            0,
            0,
            vec![endpoint],
            format,
            vec![],
        )
        .await;
        set.set_test_storage_class_config(
            storageclass::lookup_config_for_pools_without_env(&rustfs_config::server_config::KVS::new(), &[1])
                .expect("test storage class should resolve for one local drive"),
        );
        (dir, set)
    }

    async fn non_trash_tmp_entries(temp_dirs: &[TempDir]) -> Vec<String> {
        let mut entries = Vec::new();
        for dir in temp_dirs {
            let tmp = dir.path().join(RUSTFS_META_TMP_BUCKET);
            let mut read_dir = match tokio::fs::read_dir(&tmp).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => panic!("tmp directory should be readable: {err}"),
            };
            while let Some(entry) = read_dir.next_entry().await.expect("tmp entry should be readable") {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name != ".trash" {
                    entries.push(name);
                }
            }
        }
        entries
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_rename_outcome_matrix_reports_partial_and_retries_failed_targets() {
        for (case, failed_attempts, expect_error) in [
            ("ok-ok", Vec::new(), false),
            ("ok-err", vec![1], true),
            ("err-ok", vec![0], true),
            ("err-err", vec![0, 1], true),
        ] {
            let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = format!("heal-rename-{case}");
            let object = "object.bin";
            for disk in &disks {
                disk.make_volume(&bucket).await.expect("bucket volume should be created");
            }

            let payload = vec![0x5a; 1024 * 1024];
            let mut reader = PutObjReader::from_vec(payload);
            set.put_object(&bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("source object should be written");
            let source = disks[2]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("source metadata should be readable");
            let data_dir = source.data_dir.expect("non-inline source should have a data directory");
            let tmp_entries_before_heal = non_trash_tmp_entries(&temp_dirs).await;
            let target_slots = {
                let mut slots = [source.erasure.distribution[0] - 1, source.erasure.distribution[1] - 1];
                slots.sort_unstable();
                slots
            };
            let failed_slots = failed_attempts
                .iter()
                .map(|attempt| target_slots[*attempt])
                .collect::<Vec<_>>();
            let failed_physical_indexes = [0, 1]
                .into_iter()
                .filter(|index| failed_slots.contains(&(source.erasure.distribution[*index] - 1)))
                .collect::<Vec<_>>();

            for index in [0, 1] {
                tokio::fs::remove_file(
                    temp_dirs[index]
                        .path()
                        .join(&bucket)
                        .join(object)
                        .join(data_dir.to_string())
                        .join("part.1"),
                )
                .await
                .expect("target shard should be removed before heal");
            }

            let failure_scope = HealRenameFailureScope::install(&bucket, object, &failed_slots);
            let (first_result, first_error) = set
                .heal_object(
                    &bucket,
                    object,
                    "",
                    &HealOpts {
                        no_lock: true,
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
                .expect("heal should report its per-target rename outcome");
            drop(failure_scope);

            assert_eq!(first_error.is_some(), expect_error, "{case}: aggregate status must match target outcomes");
            if let Some(error) = first_error {
                let error = error.to_string();
                assert!(
                    error.contains(HEAL_RENAME_INCOMPLETE),
                    "{case}: partial/all failure must have an explicit retryable status: {error}"
                );
                assert!(
                    error.contains(&format!("{} of 2 targets committed", 2 - failed_slots.len())),
                    "{case}: aggregate status must distinguish partial from all-target failure: {error}"
                );
            }
            for index in [0, 1] {
                let expected = if failed_physical_indexes.contains(&index) {
                    DriveState::Missing
                } else {
                    DriveState::Ok
                };
                assert_eq!(
                    first_result.after.drives[index].state,
                    expected.to_string(),
                    "{case}: after.drives must reflect the actual rename outcome at index {index}"
                );
                assert_eq!(
                    temp_dirs[index]
                        .path()
                        .join(&bucket)
                        .join(object)
                        .join(data_dir.to_string())
                        .join("part.1")
                        .exists(),
                    !failed_physical_indexes.contains(&index),
                    "{case}: tmp cleanup must neither delete committed shards nor expose failed targets"
                );
            }
            let tmp_entries_after_heal = non_trash_tmp_entries(&temp_dirs).await;
            assert!(
                tmp_entries_after_heal
                    .iter()
                    .all(|entry| tmp_entries_before_heal.contains(entry)),
                "{case}: first heal must not leave a new temporary shard: {tmp_entries_after_heal:?}"
            );

            if !failed_slots.is_empty() {
                let (retry_result, retry_error) = set
                    .heal_object(
                        &bucket,
                        object,
                        "",
                        &HealOpts {
                            no_lock: true,
                            scan_mode: HealScanMode::Deep,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("second heal should retry failed targets");
                assert!(retry_error.is_none(), "{case}: second heal should complete remaining targets");
                for index in [0, 1] {
                    assert_eq!(
                        retry_result.after.drives[index].state,
                        DriveState::Ok.to_string(),
                        "{case}: second heal must converge target {index}"
                    );
                }
                let tmp_entries_after_retry = non_trash_tmp_entries(&temp_dirs).await;
                assert!(
                    tmp_entries_after_retry
                        .iter()
                        .all(|entry| tmp_entries_before_heal.contains(entry)),
                    "{case}: retry must not leave a new temporary shard: {tmp_entries_after_retry:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn replacement_target_readback_requires_the_committed_shard() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "replacement-target-readback";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(vec![0x5a; 1024 * 1024]);
        set.put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let source = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata should be readable");
        let data_dir = source.data_dir.expect("non-inline source should have a data directory");
        let targets = vec![set.set_endpoints[0].to_string(), set.set_endpoints[1].to_string()];

        assert!(
            set.replacement_targets_have_version(bucket, object, "", &targets)
                .await
                .expect("healthy target shards should be readable")
        );

        tokio::fs::remove_file(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1"),
        )
        .await
        .expect("target shard should be removed after the initial commit");

        assert!(
            !set.replacement_targets_have_version(bucket, object, "", &targets)
                .await
                .expect("missing target shard should be observable")
        );
    }

    #[tokio::test]
    async fn replacement_target_readback_checks_the_requested_historical_version() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "replacement-target-readback-versioned";
        let object = "object.bin";
        set.make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("versioned bucket should be created");

        let mut old_reader = PutObjReader::from_vec(vec![0x5a; 1024 * 1024]);
        let old_info = set
            .put_object(
                bucket,
                object,
                &mut old_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("old object version should be written");
        let old_version = old_info
            .version_id
            .expect("versioned put should return the old version id")
            .to_string();
        let mut latest_reader = PutObjReader::from_vec(vec![0x33; 1024 * 1024]);
        let latest_info = set
            .put_object(
                bucket,
                object,
                &mut latest_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("latest object version should be written");
        let latest_version = latest_info
            .version_id
            .expect("versioned put should return the latest version id")
            .to_string();
        let old_source = disks[2]
            .read_version("", bucket, object, &old_version, &ReadOptions::default())
            .await
            .expect("old version metadata should be readable");
        let old_data_dir = old_source.data_dir.expect("old version should have a data directory");
        let targets = vec![set.set_endpoints[0].to_string(), set.set_endpoints[1].to_string()];

        assert!(
            set.replacement_targets_have_version(bucket, object, &old_version, &targets)
                .await
                .expect("healthy historical target shards should be readable")
        );
        assert!(
            set.replacement_targets_have_version(bucket, object, &latest_version, &targets)
                .await
                .expect("healthy latest target shards should be readable")
        );

        tokio::fs::remove_file(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(old_data_dir.to_string())
                .join("part.1"),
        )
        .await
        .expect("old target shard should be removed after the initial commit");

        assert!(
            !set.replacement_targets_have_version(bucket, object, &old_version, &targets)
                .await
                .expect("missing old target shard should be observable")
        );
        assert!(
            set.replacement_targets_have_version(bucket, object, &latest_version, &targets)
                .await
                .expect("latest target evidence should remain independent")
        );
    }

    #[tokio::test]
    async fn format_heal_cached_layout_rejects_a_disk_from_another_slot() {
        let mut _temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for disk_index in 0..3 {
            let (temp_dir, mut endpoint, disk) = real_disk().await;
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            _temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }
        let set = set_disks_with(disks.clone(), endpoints, 1).await;
        let mut wrong_slot = set.format.clone();
        wrong_slot.erasure.this = set.format.erasure.sets[0][1];
        save_format_file(&disks[0], &Some(wrong_slot))
            .await
            .expect("wrong-slot format fixture should be saved");
        let mut correct_slot = set.format.clone();
        correct_slot.erasure.this = set.format.erasure.sets[0][2];
        save_format_file(&disks[2], &Some(correct_slot))
            .await
            .expect("correct format fixture should be saved");

        let (_, heal_err) = set
            .heal_format(false)
            .await
            .expect("format heal should report the quorum failure in its result");

        assert!(matches!(heal_err, Some(Error::CorruptedFormat)));
        let unformatted = load_format_erasure(disks[1].as_ref().expect("second disk should be online"), true)
            .await
            .expect_err("a rejected fallback must not format the missing slot");
        assert_eq!(unformatted, DiskError::UnformattedDisk);
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

    #[tokio::test]
    async fn dangling_object_dir_delete_preserves_results_and_propagates_failure() {
        let bucket = "bucket-dangling-dir-delete";
        let object = "dangling__XLDIR__";
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for _ in 0..8 {
            let (temp_dir, endpoint, disk) = real_disk().await;
            disk.make_volume(bucket).await.expect("test bucket should be created");
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }
        disks[0] = None;
        let set = set_disks_with(disks, endpoints, 4).await;
        for disk_index in [1, 2] {
            tokio::fs::create_dir_all(temp_dirs[disk_index].path().join(bucket).join(object))
                .await
                .expect("dangling object directory should be created");
        }

        let _delete_failure = DanglingDeleteFailure::install(bucket, object, 2, DiskError::DiskAccessDenied);
        let _file_missing = DanglingDeleteFailure::install(bucket, object, 3, DiskError::FileNotFound);
        let _version_missing = DanglingDeleteFailure::install(bucket, object, 4, DiskError::FileVersionNotFound);
        let _path_missing = DanglingDeleteFailure::install(bucket, object, 5, DiskError::PathNotFound);
        let _volume_missing = DanglingDeleteFailure::install(bucket, object, 6, DiskError::VolumeNotFound);
        let _disk_missing = DanglingDeleteFailure::install(bucket, object, 7, DiskError::DiskNotFound);

        let (result, err) = set
            .heal_object_dir_locked(bucket, object, false, true)
            .await
            .expect("dangling directory heal should report its per-disk delete results");

        assert_eq!(err, Some(DiskError::DiskNotFound));
        assert_eq!(result.before.drives.len(), 8);
        assert_eq!(result.after.drives.len(), 8);
        assert_eq!(result.before.drives[0].state, DriveState::Offline.to_string());
        assert_eq!(result.after.drives[0].state, DriveState::Offline.to_string());
        assert_eq!(result.before.drives[1].state, DriveState::Ok.to_string());
        assert_eq!(result.after.drives[1].state, DriveState::Missing.to_string());
        assert_eq!(result.before.drives[2].state, DriveState::Ok.to_string());
        assert_eq!(result.after.drives[2].state, DriveState::Corrupt.to_string());
        assert_eq!(result.before.drives[3].state, DriveState::Missing.to_string());
        assert_eq!(result.after.drives[3].state, DriveState::Missing.to_string());
        for disk_index in [4, 5, 6] {
            assert_eq!(result.before.drives[disk_index].state, DriveState::Missing.to_string());
            assert_eq!(result.after.drives[disk_index].state, DriveState::Corrupt.to_string());
        }
        assert_eq!(result.before.drives[7].state, DriveState::Missing.to_string());
        assert_eq!(result.after.drives[7].state, DriveState::Offline.to_string());
        assert!(
            !temp_dirs[1].path().join(bucket).join(object).exists(),
            "successful delete must remove the dangling directory"
        );
        assert!(
            temp_dirs[2].path().join(bucket).join(object).is_dir(),
            "failed delete must leave the dangling directory for retry"
        );
    }

    #[tokio::test]
    async fn dangling_delete_guard_preserves_conflicting_identities_without_writing_metadata() {
        let bucket = "bucket-delete-guard-conflict";
        let object = "object.bin";
        let old_data_dir = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("new data dir should parse");
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(old_data_dir, 4), (new_data_dir, 2)]).await;
        let version_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("version id should parse");
        let mut metadata = vec![
            meta_regen_test_fileinfo(object, old_data_dir, 9, 0),
            meta_regen_test_fileinfo(object, new_data_dir, 10, 1),
            FileInfo::default(),
            FileInfo::default(),
        ];
        metadata[0].version_id = Some(version_id);
        metadata[1].version_id = Some(version_id);
        assert_eq!(
            metadata[0].version_id, metadata[1].version_id,
            "the conflicting candidates must share one version id"
        );
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata[0]).await;
        seed_meta_regen_test_metadata(&disks, 1, bucket, object, &metadata[1]).await;
        let errs = vec![None, None, Some(DiskError::FileNotFound), Some(DiskError::FileNotFound)];

        assert!(
            set.dangling_delete_safety(bucket, object, &metadata, &errs, &disks)
                .await
                .expect("conflicting identities should be classified")
                == DanglingDeleteSafety::UnsafeToDelete
        );
        let reversed = vec![
            metadata[1].clone(),
            metadata[0].clone(),
            FileInfo::default(),
            FileInfo::default(),
        ];
        assert!(
            set.dangling_delete_safety(bucket, object, &reversed, &errs, &disks)
                .await
                .expect("reversed identities should be classified")
                == DanglingDeleteSafety::UnsafeToDelete
        );
        let version_id = version_id.to_string();
        assert!(
            !set.try_regenerate_explicit_version_meta(bucket, object, &version_id, &metadata, &errs, &disks)
                .await
                .expect("conflicting explicit-version candidates should be rejected"),
            "an explicit version must not select between conflicting metadata identities"
        );
        for disk_index in [2, 3] {
            assert!(
                matches!(
                    disks[disk_index]
                        .as_ref()
                        .expect("test disk should be online")
                        .read_version("", bucket, object, "", &ReadOptions::default())
                        .await,
                    Err(DiskError::FileNotFound)
                ),
                "the delete guard must not manufacture metadata on missing disks"
            );
        }
        let old = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("old metadata should remain readable");
        let new = disks[1]
            .as_ref()
            .expect("second test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("new metadata should remain readable");
        assert_eq!(old.data_dir, Some(old_data_dir));
        assert_eq!(new.data_dir, Some(new_data_dir));
    }

    #[tokio::test]
    async fn heal_meta_quorum_failure_preserves_reconstructable_uncommitted_candidate() {
        let bucket = "bucket-delete-guard-reconstructable";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("33333333-3333-3333-3333-333333333333").expect("data dir should parse");
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 2)]).await;
        let metadata = [
            meta_regen_test_fileinfo(object, data_dir, 3, 0),
            FileInfo::default(),
            FileInfo::default(),
            FileInfo::default(),
        ];
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata[0]).await;
        let (observed_metadata, observed_errs) = SetDisks::read_all_fileinfo(&disks, "", bucket, object, "", true, true, false)
            .await
            .expect("test metadata should be readable across the set");
        assert_eq!(
            set.dangling_delete_safety(bucket, object, &observed_metadata, &observed_errs, &disks)
                .await
                .expect("observed reconstructable candidate should be classified"),
            DanglingDeleteSafety::UnsafeToDelete
        );

        let (_, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("unsafe dangling state should be reported without deletion");
        assert_eq!(err, Some(DiskError::FileNotFound));
        let surviving = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("the only metadata copy must be preserved");
        assert_eq!(surviving.data_dir, Some(data_dir));
        assert!(
            matches!(
                disks[1]
                    .as_ref()
                    .expect("second test disk should be online")
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await,
                Err(DiskError::FileNotFound)
            ),
            "the delete guard must not propagate metadata"
        );
    }

    #[tokio::test]
    async fn heal_meta_quorum_failure_preserves_candidate_when_required_shard_disk_is_offline() {
        let bucket = "bucket-delete-guard-offline";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("data dir should parse");
        let (temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 2)]).await;
        let metadata = meta_regen_test_fileinfo(object, data_dir, 4, 0);
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata).await;
        set.disks.write().await[1] = None;

        let (_, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("offline shard state should be reported without deletion");
        assert_eq!(err, Some(DiskError::FileNotFound));
        let surviving = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("offline uncertainty must preserve the surviving metadata");
        assert_eq!(surviving.data_dir, Some(data_dir));
        assert!(
            temp_dirs[0]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1")
                .is_file(),
            "offline uncertainty must preserve the last online shard"
        );
    }

    #[tokio::test]
    async fn heal_meta_quorum_failure_preserves_candidate_when_part_probe_times_out() {
        let bucket = "bucket-delete-guard-timeout";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("data dir should parse");
        let (temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 2)]).await;
        let metadata = meta_regen_test_fileinfo(object, data_dir, 5, 0);
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata).await;
        let _failure = DanglingCheckPartsFailure::install(bucket, object, 1, DiskError::Timeout);

        let (_, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("part probe timeout should be reported without deletion");
        assert_eq!(err, Some(DiskError::FileNotFound));
        let surviving = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("probe uncertainty must preserve the surviving metadata");
        assert_eq!(surviving.data_dir, Some(data_dir));
        assert!(
            temp_dirs[0]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1")
                .is_file(),
            "probe uncertainty must preserve the last confirmed shard"
        );
    }

    #[tokio::test]
    async fn dangling_delete_guard_ignores_set_incompatible_geometry() {
        let bucket = "bucket-delete-guard-short-geometry";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("abababab-abab-abab-abab-abababababab").expect("data dir should parse");
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 1)]).await;
        let mut candidate = FileInfo::new(object, 1, 0);
        candidate.data_dir = Some(data_dir);
        candidate.mod_time = Some(OffsetDateTime::from_unix_timestamp(18).expect("timestamp should parse"));
        candidate.size = 1;
        candidate.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        candidate.erasure.index = candidate.erasure.distribution[0];
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &candidate).await;
        let metadata = vec![candidate, FileInfo::default(), FileInfo::default(), FileInfo::default()];
        let errs = vec![
            None,
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
        ];

        assert!(
            set.dangling_delete_safety(bucket, object, &metadata, &errs, &disks)
                .await
                .expect("set-incompatible geometry should be classified")
                == DanglingDeleteSafety::NoRecoverableCandidate
        );
    }

    #[tokio::test]
    async fn dangling_delete_guard_preserves_delete_marker_and_remote_metadata() {
        let bucket = "bucket-delete-guard-nonlocal";
        let object = "object.bin";
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[]).await;
        let marker = FileInfo {
            name: object.to_string(),
            version_id: Some(Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").expect("version id should parse")),
            deleted: true,
            mod_time: Some(OffsetDateTime::from_unix_timestamp(14).expect("marker timestamp should parse")),
            ..Default::default()
        };
        let remote_dir = Uuid::parse_str("89898989-8989-8989-8989-898989898989").expect("remote data dir should parse");
        let mut remote = meta_regen_test_fileinfo(object, remote_dir, 15, 1);
        remote.transition_status = TRANSITION_COMPLETE.to_string();
        remote.transition_tier = "WARM".to_string();
        remote.transitioned_objname = "remote/object.bin".to_string();

        for metadata in [marker, remote] {
            let candidates = vec![metadata, FileInfo::default(), FileInfo::default(), FileInfo::default()];
            let errs = vec![
                None,
                Some(DiskError::FileNotFound),
                Some(DiskError::FileNotFound),
                Some(DiskError::FileNotFound),
            ];
            assert_eq!(
                set.dangling_delete_safety(bucket, object, &candidates, &errs, &disks)
                    .await
                    .expect("non-local metadata should be classified"),
                DanglingDeleteSafety::UnsafeToDelete
            );
        }
    }

    #[tokio::test]
    async fn dangling_delete_guard_preserves_metadata_read_uncertainty() {
        let bucket = "bucket-delete-guard-read-error";
        let object = "object.bin";
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[]).await;
        let metadata = vec![FileInfo::default(); disks.len()];

        for read_error in [DiskError::Timeout, DiskError::DiskAccessDenied, DiskError::DiskNotFound] {
            let mut errs = vec![Some(DiskError::FileNotFound); disks.len()];
            errs[0] = Some(read_error);
            assert_eq!(
                set.dangling_delete_safety(bucket, object, &metadata, &errs, &disks)
                    .await
                    .expect("metadata read uncertainty should be classified"),
                DanglingDeleteSafety::UnsafeToDelete
            );
        }
    }

    #[tokio::test]
    async fn heal_no_parity_bitrot_reports_unrecoverable_integrity_failure() {
        let (dir, set) = formatted_single_disk_no_parity_set().await;
        let bucket = "bucket-no-parity-bitrot";
        let object = "bad-object.bin";
        let payload = (0..(BLOCK_SIZE_V2 + 17)).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload);
        set.put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("object should be written");

        let snapshot = set
            .get_object_fileinfo(bucket, object, &opts, true, false)
            .await
            .expect("object metadata should resolve");
        let fi = snapshot.fi();
        assert_eq!(fi.erasure.parity_blocks, 0);
        let data_dir = fi.data_dir.expect("non-inline object should have a data directory");
        let part_path = dir.path().join(bucket).join(object).join(data_dir.to_string()).join("part.1");
        let mut part = tokio::fs::read(&part_path).await.expect("part should be readable");
        part[0] ^= 0xff;
        tokio::fs::write(&part_path, part)
            .await
            .expect("part corruption should be written");

        let (result, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("heal should report the unrecoverable object without panicking");

        assert_eq!(err, Some(DiskError::FileCorrupt));
        assert_eq!(result.bucket, bucket);
        assert_eq!(result.object, object);
        assert_eq!(result.data_blocks, 1);
        assert_eq!(result.parity_blocks, 0);
        assert_eq!(result.before.drives[0].state, DriveState::Corrupt.to_string());
        assert!(result.detail.contains("no-parity object is unrecoverable"));
        assert!(result.detail.contains("part 1"));
        assert!(result.detail.contains("bitrot_failure=true"));
    }

    // HS-12 (backlog#1874): a versioned DELETE racing an object heal must never
    // resurrect the deleted version. The heal has real reconstruction work (a
    // shard of the doomed version is removed), so both sides touch the same
    // (bucket, object, data_dir); whichever order the ns write lock serializes
    // them in, the committed delete must win.
    #[tokio::test]
    #[serial_test::serial]
    async fn heal_racing_version_delete_never_resurrects_the_deleted_version() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-race-delete-no-resurrect";
        let object = "object.bin";
        set.make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("versioned bucket should be created");

        let mut first_reader = PutObjReader::from_vec(vec![0x11; 1024 * 1024]);
        let first_info = set
            .put_object(
                bucket,
                object,
                &mut first_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("first version should be written");
        let first_version = first_info
            .version_id
            .expect("versioned put should return the first version id")
            .to_string();

        let mut second_reader = PutObjReader::from_vec(vec![0x22; 1024 * 1024]);
        let second_info = set
            .put_object(
                bucket,
                object,
                &mut second_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("second version should be written");
        let second_version = second_info
            .version_id
            .expect("versioned put should return the second version id")
            .to_string();

        // Damage one shard of the doomed version so the racing heal performs an
        // actual reconstruction over its data dir instead of an early exit.
        let doomed_source = disks[0]
            .read_version("", bucket, object, &first_version, &ReadOptions::default())
            .await
            .expect("doomed version metadata should be readable");
        let doomed_data_dir = doomed_source
            .data_dir
            .expect("non-inline version should have a data directory");
        tokio::fs::remove_file(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(doomed_data_dir.to_string())
                .join("part.1"),
        )
        .await
        .expect("shard damage should be injected before the race");

        let delete_set = set.clone();
        let (delete_res, heal_res) = tokio::join!(
            async {
                delete_set
                    .delete_object(
                        bucket,
                        object,
                        ObjectOptions {
                            versioned: true,
                            version_id: Some(first_version.clone()),
                            object_lock_config_snapshot: Some(Arc::new(crate::set_disk::ObjectLockConfigSnapshot::new(
                                crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
                            ))),
                            ..Default::default()
                        },
                    )
                    .await
            },
            async {
                set.heal_object(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
            },
        );
        delete_res.expect("version delete must succeed under lock serialization");
        // The heal may legitimately report a transient failure when the version
        // it was rebuilding disappears mid-flight; only the end state matters.
        drop(heal_res);

        let resurrected = set
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(first_version.clone()),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(&resurrected, Err(Error::FileVersionNotFound) | Err(Error::ObjectNotFound(..))),
            "a racing heal must not resurrect the deleted version: {resurrected:?}"
        );

        let survivor = set
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(second_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("surviving version must remain readable after the race");
        assert_eq!(survivor.size, 1024 * 1024, "survivor size must be intact");
    }

    // HS-12 (backlog#1874): unversioned overwrite commits race a Deep heal on
    // the same object. The overwrite's post-commit tail deletes the replaced
    // data dir without the ns lock (object.rs commit tail), which is exactly
    // the intersection the audit flagged: the heal must tolerate the tail race
    // (retryable outcome) and every committed overwrite must survive — the
    // final current version is exactly the last payload written.
    #[tokio::test]
    #[serial_test::serial]
    async fn heal_racing_unversioned_overwrites_preserves_the_last_commit() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-race-put-overwrite";
        let object = "object.bin";
        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        const ROUNDS: usize = 8;
        const PAYLOAD_SIZE: usize = 256 * 1024;
        let mut last_etag = String::new();
        for round in 0..ROUNDS {
            // Give the heal something to rebuild on alternating rounds: remove a
            // shard of the current data dir right before the race.
            if round % 2 == 1 {
                let current = disks[2]
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .expect("current metadata should be readable");
                if let Some(data_dir) = current.data_dir {
                    let shard = temp_dirs[3]
                        .path()
                        .join(bucket)
                        .join(object)
                        .join(data_dir.to_string())
                        .join("part.1");
                    if shard.exists() {
                        tokio::fs::remove_file(&shard)
                            .await
                            .expect("shard damage should be injectable mid-race");
                    }
                }
            }

            let payload = vec![round as u8; PAYLOAD_SIZE];
            let mut put_reader = PutObjReader::from_vec(payload);
            let put_opts = ObjectOptions::default();
            let heal_opts = HealOpts {
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            };
            let (put_res, heal_res) = tokio::join!(
                set.put_object(bucket, object, &mut put_reader, &put_opts),
                set.heal_object(bucket, object, "", &heal_opts),
            );
            let put_info = put_res.expect("overwrite must succeed under lock serialization");
            last_etag = put_info.etag.clone().unwrap_or_default();
            // Heal outcome is unconstrained (may hit the tail race and report a
            // retryable error); the invariant is checked on the end state.
            drop(heal_res);
        }

        let final_info = set
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("object must remain readable after the race loop");
        assert_eq!(
            final_info.size, PAYLOAD_SIZE as i64,
            "final current version must be the last committed overwrite"
        );
        assert_eq!(
            final_info.etag.unwrap_or_default(),
            last_etag,
            "the racing heal loop must never leave a stale or resurrected current version"
        );
    }
}
