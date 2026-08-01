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

use crate::config::storageclass;
use crate::disk::error_reduce::{count_errs, reduce_write_quorum_errs};
use crate::disk::{self, DiskAPI};
use crate::error::{Error, Result};
use crate::runtime::instance::InstanceContext;
use crate::runtime::sources::{quarantine_local_disks, reconcile_local_disk_ids};
use crate::{
    disk::{
        DiskInfoOptions, DiskOption, DiskStore, FORMAT_CONFIG_FILE, MIGRATING_META_BUCKET, RUSTFS_META_BUCKET,
        error::DiskError,
        format::{FormatBackend, FormatErasureVersion, FormatMetaVersion, FormatV3},
        new_disk,
    },
    layout::endpoints::Endpoints,
};
use futures::{future::join_all, stream, stream::StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const LEGACY_FORMAT_READ_CONCURRENCY: usize = 16;
const LEGACY_FORMAT_BASE_MAX_BYTES: usize = 16 * 1024;
const LEGACY_FORMAT_MAX_BYTES_PER_DISK: usize = 64;

pub async fn init_disks(eps: &Endpoints, opt: &DiskOption) -> (Vec<Option<DiskStore>>, Vec<Option<DiskError>>) {
    let mut futures = Vec::with_capacity(eps.as_ref().len());

    for ep in eps.as_ref().iter() {
        futures.push(new_disk(ep, opt));
    }

    let mut res = Vec::with_capacity(eps.as_ref().len());
    let mut errors = Vec::with_capacity(eps.as_ref().len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(s) => {
                res.push(Some(s));
                errors.push(None);
            }
            Err(e) => {
                res.push(None);
                errors.push(Some(e));
            }
        }
    }

    (res, errors)
}

#[cfg(test)]
pub async fn connect_load_init_formats(
    first_disk: bool,
    disks: &mut [Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3> {
    let instance_ctx = crate::runtime::global::current_ctx();
    connect_load_init_formats_with_instance_ctx(&instance_ctx, first_disk, disks, set_count, set_drive_count, deployment_id).await
}

pub(crate) async fn connect_load_init_formats_with_instance_ctx(
    instance_ctx: &Arc<InstanceContext>,
    first_disk: bool,
    disks: &mut [Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3> {
    let (formats, errs) = load_format_erasure_all(disks, false).await;

    check_disk_fatal_errs(&errs)?;

    let all_unformatted = should_init_erasure_disks(&errs);
    let formats_present = formats.iter().flatten().count();
    let mut format_quorum = (formats_present > 0).then(|| select_format_erasure_in_quorum(&formats, 0));
    if format_quorum.as_ref().is_none_or(Result::is_err)
        && errs.iter().any(|error| {
            matches!(
                error,
                Some(DiskError::InconsistentDisk | DiskError::CorruptedFormat | DiskError::CorruptedBackend)
            )
        })
    {
        return Err(Error::CorruptedFormat);
    }
    let resumable_partial_migration = formats_present > 0
        && errs.iter().any(|error| matches!(error, Some(DiskError::UnformattedDisk)))
        && format_quorum.as_ref().is_some_and(Result::is_err)
        && formats.iter().zip(&errs).all(|(format, error)| {
            format.is_some() || matches!(error, Some(DiskError::UnformattedDisk | DiskError::DiskNotFound))
        });

    if first_disk && (all_unformatted || resumable_partial_migration) {
        info!(all_unformatted, resumable_partial_migration, "checking for a legacy storage format");
        match try_migrate_format(disks, &formats, set_count, set_drive_count).await {
            Ok(LegacyFormatOutcome::Migrated { format, quorum_members }) => {
                info!("Migrated format from MinIO config");
                retain_format_quorum_members(instance_ctx, disks, &format, &quorum_members, set_drive_count).await?;
                return Ok(*format);
            }
            Ok(LegacyFormatOutcome::Incompatible) => {
                error!(
                    event = "legacy_format_migration_rejected",
                    component = "ecstore",
                    subsystem = "store_init",
                    state = "incompatible",
                    "detected MinIO format.json but could not migrate it safely"
                );
                return Err(Error::CorruptedFormat);
            }
            Ok(LegacyFormatOutcome::None) => {}
            Err(e) => return Err(e),
        }
        if all_unformatted {
            let fm = init_format_erasure(instance_ctx, disks, set_count, set_drive_count, deployment_id).await?;
            return Ok(fm);
        }
    }

    info!(
        "first_disk: {}, should_init_erasure_disks: {}",
        first_disk,
        should_init_erasure_disks(&errs)
    );

    let unformatted = quorum_unformatted_disks(&errs);
    if unformatted && !first_disk {
        return Err(Error::NotFirstDisk);
    }

    if unformatted && first_disk {
        return Err(Error::FirstDiskWait);
    }

    let (fm, quorum_members) = match format_quorum.take() {
        Some(result) => result?,
        None => select_format_erasure_in_quorum(&formats, 0)?,
    };
    check_format_erasure_value_for_topology(&fm, formats.len(), set_drive_count)?;
    retain_format_quorum_members(instance_ctx, disks, &fm, &quorum_members, set_drive_count).await?;

    Ok(fm)
}

async fn retain_format_quorum_members(
    instance_ctx: &Arc<InstanceContext>,
    disks: &mut [Option<DiskStore>],
    format: &FormatV3,
    quorum_members: &[bool],
    set_drive_count: usize,
) -> Result<()> {
    if set_drive_count == 0 || quorum_members.len() != disks.len() {
        return Err(Error::CorruptedFormat);
    }

    let pool_idx = disks
        .iter()
        .flatten()
        .map(|disk| disk.endpoint().pool_idx)
        .find_map(|pool_idx| usize::try_from(pool_idx).ok());
    let registered_endpoints = if let Some(pool_idx) = pool_idx {
        instance_ctx
            .local_disk_set_drives()
            .read()
            .await
            .get(pool_idx)
            .map(|sets| {
                sets.iter()
                    .flat_map(|set| set.iter())
                    .map(|disk| disk.as_ref().map(|disk| disk.endpoint()))
                    .collect::<Vec<_>>()
            })
            .filter(|endpoints| endpoints.len() == disks.len())
    } else {
        None
    };
    let endpoints = disks
        .iter()
        .enumerate()
        .map(|(index, disk)| {
            disk.as_ref()
                .map(|disk| disk.endpoint())
                .or_else(|| registered_endpoints.as_ref()?.get(index)?.clone())
        })
        .collect::<Vec<_>>();

    let mut member_disk_ids = vec![None; disks.len()];
    let mut local_pool_endpoints = Vec::new();
    let mut selected_local_disk_ids = Vec::new();
    let mut quarantined_endpoints = Vec::new();
    for (index, ((disk, endpoint), belongs_to_quorum)) in disks.iter().zip(&endpoints).zip(quorum_members).enumerate() {
        if let Some(endpoint) = endpoint.as_ref().filter(|endpoint| endpoint.is_local) {
            local_pool_endpoints.push(endpoint.to_string());
            if !belongs_to_quorum {
                quarantined_endpoints.push(endpoint.clone());
            }
        }
        if !belongs_to_quorum {
            continue;
        }
        let disk = disk.as_ref().ok_or(Error::CorruptedFormat)?;
        let disk_id = format
            .erasure
            .sets
            .get(index / set_drive_count)
            .and_then(|set| set.get(index % set_drive_count))
            .copied()
            .ok_or(Error::CorruptedFormat)?;
        member_disk_ids[index] = Some(disk_id);
        if disk.is_local() {
            selected_local_disk_ids.push((disk_id, disk.endpoint().to_string()));
        }
    }
    quarantine_local_disks(instance_ctx, &quarantined_endpoints).await?;

    for (disk, disk_id) in disks.iter().zip(member_disk_ids) {
        if let Some(disk) = disk {
            disk.set_disk_id_state(disk_id).await?;
        }
    }
    reconcile_local_disk_ids(instance_ctx, &local_pool_endpoints, &selected_local_disk_ids).await;
    for (disk, belongs_to_quorum) in disks.iter_mut().zip(quorum_members) {
        if !belongs_to_quorum {
            *disk = None;
        }
    }
    Ok(())
}

pub fn quorum_unformatted_disks(errs: &[Option<DiskError>]) -> bool {
    count_errs(errs, &DiskError::UnformattedDisk) > (errs.len() / 2)
}

pub fn should_init_erasure_disks(errs: &[Option<DiskError>]) -> bool {
    count_errs(errs, &DiskError::UnformattedDisk) == errs.len()
}

pub fn check_disk_fatal_errs(errs: &[Option<DiskError>]) -> disk::error::Result<()> {
    if count_errs(errs, &DiskError::UnsupportedDisk) == errs.len() {
        return Err(DiskError::UnsupportedDisk);
    }

    if count_errs(errs, &DiskError::FileAccessDenied) == errs.len() {
        return Err(DiskError::FileAccessDenied);
    }

    if count_errs(errs, &DiskError::DiskNotDir) == errs.len() {
        return Err(DiskError::DiskNotDir);
    }

    Ok(())
}

async fn init_format_erasure(
    instance_ctx: &Arc<InstanceContext>,
    disks: &mut [Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3> {
    let fm = FormatV3::new(set_count, set_drive_count);
    let mut fms = vec![None; disks.len()];
    for i in 0..set_count {
        for j in 0..set_drive_count {
            let idx = i * set_drive_count + j;
            let mut newfm = fm.clone();
            newfm.erasure.this = fm.erasure.sets[i][j];
            if let Some(id) = deployment_id {
                newfm.id = id;
            }

            fms[idx] = Some(newfm);
        }
    }

    write_format_file_all(disks, &fms).await?;
    let format = get_format_erasure_in_quorum(&fms, 0)?;
    let quorum_members = vec![true; disks.len()];
    retain_format_quorum_members(instance_ctx, disks, &format, &quorum_members, set_drive_count).await?;
    Ok(format)
}

/// Outcome of attempting to migrate an on-disk MinIO `format.json`.
enum LegacyFormatOutcome {
    /// A compatible MinIO format was found and migrated into RustFS format files.
    /// Boxed to keep the enum small (`FormatV3` is large; the others are unit variants).
    Migrated {
        format: Box<FormatV3>,
        quorum_members: Vec<bool>,
    },
    /// A MinIO `format.json` was present but could not be migrated (topology /
    /// version mismatch, or a parse failure). The caller must decide how to
    /// proceed; creating a fresh format would leave the legacy objects unreadable.
    Incompatible,
    /// No MinIO `format.json` was present on any disk (a normal fresh install).
    None,
}

/// Tries to migrate an on-disk MinIO `format.json` into RustFS format files.
///
/// Returns [`LegacyFormatOutcome`] describing whether a legacy format was present
/// and, if so, whether it was compatible. `Err` is returned for read or persist
/// failures that prevent a conclusive migration decision.
async fn try_migrate_format(
    disks: &[Option<DiskStore>],
    rustfs_formats: &[Option<FormatV3>],
    set_count: usize,
    set_drive_count: usize,
) -> Result<LegacyFormatOutcome> {
    let legacy_format_max_bytes = legacy_format_max_bytes(disks.len())?;
    let mut legacy_seen = false;
    let mut invalid_legacy_format = false;
    let mut read_error: Option<Error> = None;
    let mut legacy_formats = vec![None; disks.len()];

    let reads = stream::iter(disks.iter().enumerate().map(|(index, disk)| async move {
        let Some(disk) = disk else {
            return (index, None);
        };
        let reader = match disk.read_file(MIGRATING_META_BUCKET, FORMAT_CONFIG_FILE).await {
            Ok(reader) => reader,
            Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => return (index, None),
            Err(err) => return (index, Some(Err(err))),
        };
        let data = match read_legacy_format_bytes(reader, legacy_format_max_bytes).await {
            Ok(data) => data,
            Err(err) => return (index, Some(Err(err))),
        };
        if data.is_empty() {
            return (index, Some(Ok(None)));
        }

        match FormatV3::try_from(data.as_slice()) {
            Ok(format) => (index, Some(Ok(Some(format)))),
            Err(err) => {
                warn!(
                    event = "legacy_format_decode_failed",
                    component = "ecstore",
                    subsystem = "store_init",
                    disk_index = index,
                    error = %err,
                    "failed to decode legacy storage format"
                );
                (index, Some(Ok(None)))
            }
        }
    }))
    .buffer_unordered(LEGACY_FORMAT_READ_CONCURRENCY);
    tokio::pin!(reads);
    while let Some((index, read)) = reads.next().await {
        let Some(read) = read else {
            continue;
        };
        legacy_seen = true;
        match read {
            Ok(Some(format)) => legacy_formats[index] = Some(format),
            Ok(None) => invalid_legacy_format = true,
            Err(err) => {
                read_error.get_or_insert_with(|| err.into());
            }
        }
    }

    if let Some(err) = read_error {
        return Err(err);
    }
    if !legacy_seen {
        return Ok(LegacyFormatOutcome::None);
    }
    if invalid_legacy_format {
        return Ok(LegacyFormatOutcome::Incompatible);
    }

    let format = match get_format_erasure_in_quorum(&legacy_formats, 0) {
        Ok(format) => format,
        Err(_) => return Ok(LegacyFormatOutcome::Incompatible),
    };
    if format.erasure.sets.len() != set_count
        || check_format_erasure_value_for_topology(&format, disks.len(), set_drive_count).is_err()
        || !formats_match_reference_slots(&legacy_formats, &format, 0)
        || !formats_match_reference_slots(rustfs_formats, &format, 0)
    {
        return Ok(LegacyFormatOutcome::Incompatible);
    }

    let mut formats_to_write = vec![None; disks.len()];
    for (index, disk) in disks.iter().enumerate() {
        if disk.is_some() {
            let mut disk_format = format.clone();
            disk_format.erasure.this = format.erasure.sets[index / set_drive_count][index % set_drive_count];
            formats_to_write[index] = Some(disk_format);
        }
    }

    let writes = disks
        .iter()
        .zip(&formats_to_write)
        .zip(rustfs_formats)
        .filter_map(|((disk, format), existing)| {
            if existing.is_some() {
                return None;
            }
            Some(write_format_file(disk.as_ref()?, format.as_ref()?))
        });
    let mut write_error = None;
    for result in join_all(writes).await {
        if let Err(error) = result {
            write_error.get_or_insert(error);
        }
    }
    let (persisted_formats, persisted_errors) = load_format_erasure_all(disks, false).await;
    let Some((persisted_format, quorum_members)) =
        select_persisted_migration_format(&persisted_formats, persisted_errors, &format, set_drive_count, write_error)?
    else {
        return Ok(LegacyFormatOutcome::Incompatible);
    };

    Ok(LegacyFormatOutcome::Migrated {
        format: Box::new(persisted_format),
        quorum_members,
    })
}

fn select_persisted_migration_format(
    persisted_formats: &[Option<FormatV3>],
    persisted_errors: Vec<Option<DiskError>>,
    reference: &FormatV3,
    set_drive_count: usize,
    write_error: Option<DiskError>,
) -> Result<Option<(FormatV3, Vec<bool>)>> {
    if !formats_match_reference_slots(persisted_formats, reference, 0) {
        return Ok(None);
    }
    let quorum_error = match select_format_erasure_in_quorum(persisted_formats, 0) {
        Ok((format, quorum_members)) => {
            check_format_erasure_value_for_topology(&format, persisted_formats.len(), set_drive_count)?;
            return Ok(Some((format, quorum_members)));
        }
        Err(error) => error,
    };
    if let Some(error) = persisted_errors
        .into_iter()
        .flatten()
        .find(|error| !matches!(error, DiskError::UnformattedDisk | DiskError::DiskNotFound))
    {
        return match error {
            DiskError::CorruptedFormat | DiskError::CorruptedBackend | DiskError::InconsistentDisk => Ok(None),
            error => Err(error.into()),
        };
    }
    Err(write_error.map_or(quorum_error, Into::into))
}

fn legacy_format_max_bytes(disk_count: usize) -> Result<usize> {
    disk_count
        .checked_mul(LEGACY_FORMAT_MAX_BYTES_PER_DISK)
        .and_then(|size| size.checked_add(LEGACY_FORMAT_BASE_MAX_BYTES))
        .ok_or_else(|| Error::other("legacy format size limit overflow"))
}

async fn read_legacy_format_bytes(reader: impl tokio::io::AsyncRead + Unpin, max_bytes: usize) -> disk::error::Result<Vec<u8>> {
    let read_limit = max_bytes.checked_add(1).ok_or(DiskError::CorruptedFormat)?;
    let mut data = Vec::with_capacity(read_limit.min(64 * 1024));
    reader
        .take(u64::try_from(read_limit).map_err(|_| DiskError::CorruptedFormat)?)
        .read_to_end(&mut data)
        .await?;
    if data.len() > max_bytes {
        return Err(DiskError::CorruptedFormat);
    }
    Ok(data)
}

pub(crate) fn formats_match_reference_slots(formats: &[Option<FormatV3>], reference: &FormatV3, slot_offset: usize) -> bool {
    let Ok(set_drive_count) = validate_format_erasure_layout(reference) else {
        return false;
    };

    formats.iter().enumerate().all(|(index, format)| {
        format.as_ref().is_none_or(|format| {
            format.shared_identity() == reference.shared_identity()
                && slot_offset
                    .checked_add(index)
                    .and_then(|slot| {
                        reference
                            .erasure
                            .sets
                            .get(slot / set_drive_count)
                            .and_then(|set| set.get(slot % set_drive_count))
                    })
                    .is_some_and(|expected| *expected == format.erasure.this)
        })
    })
}

pub fn get_format_erasure_in_quorum(formats: &[Option<FormatV3>], slot_offset: usize) -> Result<FormatV3> {
    select_format_erasure_in_quorum(formats, slot_offset).map(|(format, _)| format)
}

pub(crate) fn select_format_erasure_in_quorum(formats: &[Option<FormatV3>], slot_offset: usize) -> Result<(FormatV3, Vec<bool>)> {
    let mut candidates = HashMap::new();
    let formats_present = formats.iter().flatten().count();
    let required_votes = formats.len() / 2 + 1;

    for (index, format) in formats
        .iter()
        .enumerate()
        .filter_map(|(index, format)| Some((index, format.as_ref()?)))
    {
        let Some(slot) = slot_offset.checked_add(index) else {
            continue;
        };
        let (representative, set_drive_count, member_indices) = candidates
            .entry(format.shared_identity())
            .or_insert_with(|| (format, validate_format_erasure_layout(format).ok(), Vec::new()));
        let Some(set_drive_count) = *set_drive_count else {
            continue;
        };
        if representative
            .erasure
            .sets
            .get(slot / set_drive_count)
            .and_then(|set| set.get(slot % set_drive_count))
            .is_some_and(|expected| *expected == format.erasure.this)
        {
            member_indices.push(index);
        }
    }

    let candidate_groups = candidates
        .values()
        .filter(|(_, _, member_indices)| !member_indices.is_empty())
        .count();
    let log_quorum_failure = |max_votes| {
        warn!(
            event = "format_quorum_failed",
            component = "ecstore",
            subsystem = "store_init",
            state = "rejected",
            formats_total = formats.len(),
            formats_present,
            candidate_groups,
            max_votes,
            required_votes,
            "storage format quorum not reached"
        );
    };
    let Some((format, _, member_indices)) = candidates
        .into_values()
        .max_by_key(|(_, _, member_indices)| member_indices.len())
    else {
        log_quorum_failure(0);
        return Err(Error::ErasureReadQuorum);
    };

    let max_count = member_indices.len();
    if max_count < required_votes {
        log_quorum_failure(max_count);
        return Err(Error::ErasureReadQuorum);
    }

    let mut format = (*format).clone();
    format.erasure.this = Uuid::nil();
    format.disk_info = None;

    let mut member_mask = vec![false; formats.len()];
    for index in member_indices {
        member_mask[index] = true;
    }

    Ok((format, member_mask))
}

pub fn check_format_erasure_values(
    formats: &[Option<FormatV3>],
    // disks: &Vec<Option<DiskStore>>,
    set_drive_count: usize,
) -> Result<()> {
    let mut checked_identities = HashSet::new();
    for format in formats.iter().flatten() {
        // `shared_identity` contains every persisted field inspected by the
        // validators; only the per-slot UUID and runtime-only disk info differ.
        if checked_identities.insert(format.shared_identity()) {
            check_format_erasure_value_for_topology(format, formats.len(), set_drive_count)?;
        }
    }
    Ok(())
}

fn check_format_erasure_value_for_topology(format: &FormatV3, format_count: usize, set_drive_count: usize) -> Result<()> {
    let set_drive_count_in_format = validate_format_erasure_layout(format)?;
    let format_drive_count = format
        .erasure
        .sets
        .len()
        .checked_mul(set_drive_count_in_format)
        .ok_or_else(|| Error::other("erasure set drive count overflow"))?;
    if format_count != format_drive_count {
        return Err(Error::other(format!(
            "formats length for erasure.sets does not match: got {format_count}, expected {format_drive_count}"
        )));
    }
    if set_drive_count_in_format != set_drive_count {
        return Err(Error::other(format!(
            "erasure set length for set_drive_count does not match: got {set_drive_count_in_format}, expected {set_drive_count}"
        )));
    }
    Ok(())
}

fn check_format_erasure_value(format: &FormatV3) -> Result<()> {
    if format.version != FormatMetaVersion::V1 {
        return Err(Error::other("invalid FormatMetaVersion"));
    }

    if !matches!(format.format, FormatBackend::Erasure | FormatBackend::ErasureSingle) {
        return Err(Error::other("invalid FormatBackend"));
    }

    if format.id.is_nil() || format.id == Uuid::max() {
        return Err(Error::other("invalid deployment ID"));
    }

    if format.erasure.version != FormatErasureVersion::V3 {
        return Err(Error::other("invalid FormatErasureVersion"));
    }
    Ok(())
}

fn validate_format_erasure_layout(format: &FormatV3) -> Result<usize> {
    check_format_erasure_value(format)?;

    let set_drive_count = format
        .erasure
        .sets
        .first()
        .map(Vec::len)
        .filter(|count| *count > 0)
        .ok_or_else(|| Error::other("erasure.sets must contain at least one drive"))?;
    if format.erasure.sets.iter().any(|set| set.len() != set_drive_count) {
        return Err(Error::other("erasure.sets must be rectangular"));
    }
    let drive_count = format
        .erasure
        .sets
        .len()
        .checked_mul(set_drive_count)
        .ok_or_else(|| Error::other("erasure set drive count overflow"))?;
    let mut disk_ids = HashSet::with_capacity(drive_count);

    for disk_id in format.erasure.sets.iter().flatten() {
        if disk_id.is_nil() || *disk_id == Uuid::max() {
            return Err(Error::other("erasure.sets contains an invalid disk UUID"));
        }
        if !disk_ids.insert(*disk_id) {
            return Err(Error::other("erasure.sets contains a duplicate disk UUID"));
        }
    }

    Ok(set_drive_count)
}

// load_format_erasure_all reads all format.json files
pub async fn load_format_erasure_all(disks: &[Option<DiskStore>], heal: bool) -> (Vec<Option<FormatV3>>, Vec<Option<DiskError>>) {
    let mut futures = Vec::with_capacity(disks.len());
    let mut datas = Vec::with_capacity(disks.len());
    let mut errors = Vec::with_capacity(disks.len());

    for disk in disks.iter() {
        futures.push(async move {
            if let Some(disk) = disk {
                load_format_erasure(disk, heal).await
            } else {
                Err(DiskError::DiskNotFound)
            }
        });
    }

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(s) => {
                datas.push(Some(s));
                errors.push(None);
            }
            Err(e) => {
                datas.push(None);
                errors.push(Some(e));
            }
        }
    }

    // Log aggregation summary of format load results
    let ok_count = errors.iter().filter(|e| e.is_none()).count();
    let err_count = errors.iter().filter(|e| e.is_some()).count();
    // Count occurrences of each unique error
    let mut err_counts: HashMap<String, usize> = HashMap::new();
    for err in errors.iter().flatten() {
        *err_counts.entry(format!("{err}")).or_default() += 1;
    }
    if !err_counts.is_empty() {
        debug!(
            disks_ok = ok_count,
            disks_err = err_count,
            disks_total = disks.len(),
            "load format erasure all errors: {:?}",
            err_counts
        );
    }

    (datas, errors)
}

pub async fn load_format_erasure(disk: &DiskStore, heal: bool) -> disk::error::Result<FormatV3> {
    let data = disk
        .read_all(RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
        .await
        .map_err(|e| match e {
            DiskError::FileNotFound => DiskError::UnformattedDisk,
            DiskError::DiskNotFound => DiskError::UnformattedDisk,
            _ => {
                warn!("load_format_erasure err: {:?} {:?}", disk.to_string(), e);
                e
            }
        })?;

    let mut fm = FormatV3::try_from(data.as_ref())?;

    if heal {
        let info = disk
            .disk_info(&DiskInfoOptions {
                noop: heal,
                ..Default::default()
            })
            .await?;
        fm.disk_info = Some(info);
    }

    Ok(fm)
}

async fn write_format_file_all(disks: &[Option<DiskStore>], formats: &[Option<FormatV3>]) -> disk::error::Result<()> {
    let futures = disks.iter().zip(formats).map(|(disk, format)| async move {
        let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
        let format = format.as_ref().ok_or_else(|| DiskError::other("format is none"))?;
        write_format_file(disk, format).await
    });
    let mut errors = Vec::with_capacity(disks.len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(_) => {
                errors.push(None);
            }
            Err(e) => {
                errors.push(Some(e));
            }
        }
    }

    if let Some(e) = reduce_write_quorum_errs(&errors, &[], disks.len()) {
        return Err(e);
    }

    Ok(())
}

pub async fn save_format_file(disk: &Option<DiskStore>, format: &Option<FormatV3>) -> disk::error::Result<()> {
    let Some(disk) = disk else {
        return Err(DiskError::DiskNotFound);
    };

    let Some(format) = format else {
        return Err(DiskError::other("format is none"));
    };

    write_format_file(disk, format).await?;
    disk.set_disk_id(Some(format.erasure.this)).await?;

    Ok(())
}

async fn write_format_file(disk: &DiskStore, format: &FormatV3) -> disk::error::Result<()> {
    let json_data = format.to_json()?;

    let tmpfile = Uuid::new_v4().to_string();

    disk.write_all(RUSTFS_META_BUCKET, tmpfile.as_str(), json_data.into_bytes().into())
        .await?;

    disk.rename_file(RUSTFS_META_BUCKET, tmpfile.as_str(), RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
        .await?;

    Ok(())
}

pub fn ec_drives_no_config(set_drive_count: usize) -> Result<usize> {
    let parity = storageclass::default_parity_count(set_drive_count);
    storageclass::validate_parity(parity, set_drive_count)?;
    Ok(parity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoint::Endpoint;
    use crate::runtime::global::{current_ctx, reset_local_disk_test_state};
    use crate::runtime::sources::{local_disk_path_by_id, record_local_disks};
    use crate::store::peer::{find_local_disk_by_ref, prewarm_local_disk_id_map_with_instance_ctx};
    use serial_test::serial;

    async fn local_disks(count: usize) -> (tempfile::TempDir, Vec<Option<DiskStore>>) {
        let temp_dir = tempfile::tempdir().expect("temporary disk root should be created");
        let mut endpoints = Vec::with_capacity(count);
        for disk_index in 0..count {
            let path = temp_dir.path().join(format!("disk-{disk_index}"));
            tokio::fs::create_dir_all(&path)
                .await
                .expect("temporary disk path should be created");
            let mut endpoint =
                Endpoint::try_from(path.to_str().expect("temporary disk path should be UTF-8")).expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            endpoints.push(endpoint);
        }

        let (disks, errors) = init_disks(
            &Endpoints::from(endpoints),
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await;
        assert!(errors.iter().all(Option::is_none), "local disk initialization failed: {errors:?}");

        (temp_dir, disks)
    }

    async fn two_local_disks_with_missing_third() -> (tempfile::TempDir, Vec<Option<DiskStore>>) {
        let (temp_dir, mut disks) = local_disks(2).await;
        disks.push(None);

        (temp_dir, disks)
    }

    async fn write_legacy_format(disk: &Option<DiskStore>, format: &FormatV3) {
        write_legacy_bytes(disk, bytes::Bytes::from(format.to_json().expect("legacy format should serialize"))).await;
    }

    async fn write_legacy_majority(disks: &[Option<DiskStore>], format: &FormatV3) {
        for (index, disk) in disks.iter().enumerate().take(disks.len() / 2 + 1) {
            let mut disk_format = format.clone();
            disk_format.erasure.this = format.erasure.sets[0][index];
            write_legacy_format(disk, &disk_format).await;
        }
    }

    async fn write_legacy_bytes(disk: &Option<DiskStore>, data: bytes::Bytes) {
        let disk = disk.as_ref().expect("legacy disk should exist");
        if let Err(err) = disk.make_volume(MIGRATING_META_BUCKET).await {
            assert_eq!(err, DiskError::VolumeExists, "legacy metadata volume should be created");
        }
        disk.write_all(MIGRATING_META_BUCKET, FORMAT_CONFIG_FILE, data)
            .await
            .expect("legacy format should be written");
    }

    async fn write_rustfs_bytes(disk: &Option<DiskStore>, data: bytes::Bytes) {
        let disk = disk.as_ref().expect("RustFS disk should exist");
        if let Err(err) = disk.make_volume(RUSTFS_META_BUCKET).await {
            assert_eq!(err, DiskError::VolumeExists, "RustFS metadata volume should be created");
        }
        disk.write_all(RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE, data)
            .await
            .expect("RustFS format should be written");
    }

    #[tokio::test]
    async fn legacy_format_reader_rejects_oversized_input() {
        let max_bytes = 32;
        let accepted = read_legacy_format_bytes(std::io::Cursor::new(vec![0; max_bytes]), max_bytes)
            .await
            .expect("input at the limit should be accepted");
        assert_eq!(accepted.len(), max_bytes);

        let err = read_legacy_format_bytes(std::io::Cursor::new(vec![0; max_bytes + 1]), max_bytes)
            .await
            .expect_err("input above the limit must be rejected before parsing");
        assert_eq!(err, DiskError::CorruptedFormat);
    }

    #[tokio::test]
    async fn oversized_legacy_format_fails_closed_without_rustfs_writes() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let legacy = FormatV3::new(1, 3);
        let oversized_len = legacy_format_max_bytes(disks.len()).expect("size limit should resolve") + 1;
        for (index, disk) in disks.iter().enumerate().take(2) {
            let mut disk_format = legacy.clone();
            disk_format.erasure.this = legacy.erasure.sets[0][index];
            let mut data = disk_format.to_json().expect("legacy format should serialize").into_bytes();
            data.resize(oversized_len, b' ');
            write_legacy_bytes(disk, data.into()).await;
        }

        let error = connect_load_init_formats(true, &mut disks, 1, 3, None)
            .await
            .expect_err("oversized legacy metadata must be rejected");
        assert!(matches!(error, Error::CorruptedFormat), "unexpected error: {error:?}");
        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "oversized legacy metadata must not be migrated");
        assert!(
            errors.iter().all(|error| matches!(error, Some(DiskError::UnformattedDisk))),
            "oversized legacy metadata must not create RustFS format files: {errors:?}"
        );
    }

    #[test]
    fn ec_drives_no_config_uses_topology_defaults() {
        assert_eq!(ec_drives_no_config(1).expect("single-drive topology should resolve"), 0);
        assert_eq!(ec_drives_no_config(2).expect("two-drive topology should resolve"), 1);
        assert_eq!(ec_drives_no_config(6).expect("six-drive topology should resolve"), 3);
    }

    #[test]
    fn format_quorum_rejects_duplicate_sentinel_and_ragged_layouts() {
        let mut duplicate = FormatV3::new(1, 3);
        duplicate.erasure.sets[0][1] = duplicate.erasure.sets[0][0];

        let mut nil = FormatV3::new(1, 3);
        nil.erasure.sets[0][2] = Uuid::nil();

        let mut max = FormatV3::new(1, 3);
        max.erasure.sets[0][2] = Uuid::max();

        let mut ragged = FormatV3::new(2, 2);
        ragged.erasure.sets[1].pop();

        for (name, format, total_slots, voting_slots) in [
            ("duplicate", duplicate, 3, vec![0, 1]),
            ("nil", nil, 3, vec![0, 1]),
            ("max", max, 3, vec![0, 1]),
            ("ragged", ragged, 4, vec![0, 1, 2]),
        ] {
            let set_drive_count = format.erasure.sets[0].len();
            let mut formats = vec![None; total_slots];
            for index in voting_slots {
                let mut vote = format.clone();
                vote.erasure.this = format.erasure.sets[index / set_drive_count][index % set_drive_count];
                formats[index] = Some(vote);
            }

            assert!(
                matches!(get_format_erasure_in_quorum(&formats, 0), Err(Error::ErasureReadQuorum)),
                "{name} layout must not form a format quorum"
            );
            assert!(
                check_format_erasure_values(&formats, set_drive_count).is_err(),
                "{name} layout must fail startup format validation"
            );
        }
    }

    #[test]
    fn format_quorum_rejects_unknown_backend_and_invalid_deployment_ids() {
        let mut unknown_backend = FormatV3::new(1, 3);
        unknown_backend.format = FormatBackend::Unknown;

        let mut nil_deployment = FormatV3::new(1, 3);
        nil_deployment.id = Uuid::nil();

        let mut max_deployment = FormatV3::new(1, 3);
        max_deployment.id = Uuid::max();

        for (name, format) in [
            ("unknown backend", unknown_backend),
            ("nil deployment ID", nil_deployment),
            ("max deployment ID", max_deployment),
        ] {
            let mut formats = vec![None; 3];
            for (index, slot) in formats.iter_mut().take(2).enumerate() {
                let mut vote = format.clone();
                vote.erasure.this = format.erasure.sets[0][index];
                *slot = Some(vote);
            }

            assert!(
                matches!(get_format_erasure_in_quorum(&formats, 0), Err(Error::ErasureReadQuorum)),
                "{name} must not form a format quorum"
            );
        }
    }

    #[test]
    fn reference_slot_validation_rejects_wrong_slot_and_foreign_minorities() {
        let reference = FormatV3::new(1, 3);
        let mut formats = (0..3)
            .map(|index| {
                let mut format = reference.clone();
                format.erasure.this = reference.erasure.sets[0][index];
                Some(format)
            })
            .collect::<Vec<_>>();
        assert!(formats_match_reference_slots(&formats, &reference, 0));

        formats[2].as_mut().expect("third format should exist").erasure.this = reference.erasure.sets[0][0];
        assert!(!formats_match_reference_slots(&formats, &reference, 0));

        formats[2] = {
            let mut foreign = reference.clone();
            foreign.id = Uuid::new_v4();
            foreign.erasure.this = foreign.erasure.sets[0][2];
            Some(foreign)
        };
        assert!(!formats_match_reference_slots(&formats, &reference, 0));
    }

    #[tokio::test]
    async fn existing_format_load_succeeds_with_a_strict_majority() {
        let (_temp_dir, mut disks) = two_local_disks_with_missing_third().await;
        let mut format = FormatV3::new(1, 3);
        let mut expected = format.clone();
        expected.erasure.this = Uuid::nil();

        format.erasure.this = format.erasure.sets[0][0];
        save_format_file(&disks[0], &Some(format.clone()))
            .await
            .expect("existing format should be written to the first disk");
        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::ErasureReadQuorum)
        ));
        assert!(matches!(
            load_format_erasure(disks[1].as_ref().expect("second disk should exist"), false).await,
            Err(DiskError::UnformattedDisk)
        ));

        format.erasure.this = format.erasure.sets[0][1];
        save_format_file(&disks[1], &Some(format))
            .await
            .expect("existing format should be written to the second disk");

        assert_eq!(
            connect_load_init_formats(true, &mut disks, 1, 3, None)
                .await
                .expect("two existing formats should satisfy the production load path"),
            expected
        );
    }

    #[tokio::test]
    async fn existing_format_load_rejects_conflicting_formats_without_a_majority() {
        let (_temp_dir, mut disks) = two_local_disks_with_missing_third().await;
        let mut first = FormatV3::new(1, 3);
        first.erasure.this = first.erasure.sets[0][0];
        save_format_file(&disks[0], &Some(first))
            .await
            .expect("first existing format should be written");

        let mut second = FormatV3::new(1, 3);
        second.erasure.this = second.erasure.sets[0][1];
        save_format_file(&disks[1], &Some(second))
            .await
            .expect("conflicting existing format should be written");

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::ErasureReadQuorum)
        ));
    }

    #[tokio::test]
    async fn existing_format_load_excludes_disks_outside_the_selected_quorum() {
        for wrong_slot_id in [false, true] {
            let (_temp_dir, mut disks) = local_disks(3).await;
            let mut majority = FormatV3::new(1, 3);
            let mut expected = majority.clone();
            expected.erasure.this = Uuid::nil();

            for (index, disk) in disks.iter().take(2).enumerate() {
                majority.erasure.this = majority.erasure.sets[0][index];
                save_format_file(disk, &Some(majority.clone()))
                    .await
                    .expect("majority format should be written");
            }

            let mut outlier = if wrong_slot_id {
                majority.clone()
            } else {
                FormatV3::new(1, 3)
            };
            outlier.erasure.this = if wrong_slot_id {
                outlier.erasure.sets[0][0]
            } else {
                outlier.erasure.sets[0][2]
            };
            save_format_file(&disks[2], &Some(outlier))
                .await
                .expect("outlier format should be written");

            assert_eq!(
                connect_load_init_formats(true, &mut disks, 1, 3, None)
                    .await
                    .expect("two valid members should select the majority format"),
                expected
            );
            assert!(disks[0].is_some() && disks[1].is_some());
            assert!(disks[2].is_none(), "the outlier disk must not enter the selected erasure set");
        }
    }

    #[tokio::test]
    #[serial]
    async fn existing_format_load_publishes_only_validated_quorum_disk_ids() {
        reset_local_disk_test_state().await;
        let (_temp_dir, mut disks) = local_disks(5).await;
        let canonical = FormatV3::new(1, 5);
        for (index, disk) in disks.iter().enumerate().take(3) {
            let mut disk_format = canonical.clone();
            disk_format.erasure.this = canonical.erasure.sets[0][index];
            save_format_file(disk, &Some(disk_format))
                .await
                .expect("canonical format should be written");
        }
        let canonical_id = canonical.erasure.sets[0][0];
        let mut foreign = FormatV3::new(1, 5);
        foreign.erasure.this = foreign.erasure.sets[0][4];
        let foreign_id = foreign.erasure.this;
        save_format_file(&disks[4], &Some(foreign))
            .await
            .expect("foreign format should be written");
        let mut collision = FormatV3::new(1, 5);
        collision.erasure.sets[0][3] = canonical_id;
        collision.erasure.this = canonical_id;
        save_format_file(&disks[3], &Some(collision))
            .await
            .expect("foreign collision format should be written");
        let canonical_endpoint = disks[0].as_ref().expect("canonical disk should exist").endpoint().to_string();
        let collision_endpoint = disks[3].as_ref().expect("collision disk should exist").endpoint().to_string();
        let foreign_endpoint = disks[4].as_ref().expect("foreign disk should exist").endpoint().to_string();
        let prewarm_endpoints = Endpoints::from(
            disks
                .iter()
                .map(|disk| disk.as_ref().expect("test disk should exist").endpoint())
                .collect::<Vec<_>>(),
        );
        for disk in disks.iter().flatten() {
            disk.set_disk_id(None).await.expect("test disk ID should be reset");
        }
        let (prewarm_disks, prewarm_errors) = init_disks(
            &prewarm_endpoints,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await;
        assert!(prewarm_errors.iter().all(Option::is_none));
        let prewarm_disks = prewarm_disks
            .into_iter()
            .map(|disk| disk.expect("prewarm disk should exist"))
            .collect::<Vec<_>>();
        let instance_ctx = current_ctx();
        record_local_disks(&instance_ctx, prewarm_disks.clone()).await;
        *instance_ctx.local_disk_set_drives().write().await = vec![vec![prewarm_disks.into_iter().map(Some).collect()]];
        prewarm_local_disk_id_map_with_instance_ctx(&instance_ctx).await;
        instance_ctx
            .local_disk_id_map()
            .write()
            .await
            .insert(canonical_id, collision_endpoint.clone());
        assert!(local_disk_path_by_id(&foreign_id).await.is_some(), "foreign ID should be prewarmed");
        assert_eq!(local_disk_path_by_id(&canonical_id).await, Some(collision_endpoint));
        disks[4] = None;

        connect_load_init_formats(true, &mut disks, 1, 5, None)
            .await
            .expect("the canonical strict majority should load");

        assert!(disks[..3].iter().all(Option::is_some));
        assert!(disks[3..].iter().all(Option::is_none));
        assert_eq!(local_disk_path_by_id(&canonical_id).await, Some(canonical_endpoint.clone()));
        assert_eq!(local_disk_path_by_id(&foreign_id).await, None);
        assert!(
            instance_ctx
                .local_disk_map()
                .read()
                .await
                .get(&foreign_endpoint)
                .is_some_and(Option::is_none)
        );
        assert!(instance_ctx.local_disk_set_drives().read().await[0][0][4].is_none());
        assert!(find_local_disk_by_ref(&foreign_id.to_string()).await.is_none());
        assert_eq!(
            find_local_disk_by_ref(&canonical_id.to_string())
                .await
                .map(|disk| disk.endpoint().to_string()),
            Some(canonical_endpoint)
        );
        reset_local_disk_test_state().await;
    }

    #[test]
    fn persisted_migration_quorum_ignores_nonmember_read_errors() {
        for drive_count in [3, 4] {
            let reference = FormatV3::new(1, drive_count);
            let required = drive_count / 2 + 1;
            let mut formats = vec![None; drive_count];
            let mut errors = (0..drive_count).map(|_| None).collect::<Vec<Option<DiskError>>>();
            for (index, format) in formats.iter_mut().enumerate().take(required) {
                let mut disk_format = reference.clone();
                disk_format.erasure.this = reference.erasure.sets[0][index];
                *format = Some(disk_format);
            }
            errors[drive_count - 1] = Some(DiskError::FaultyDisk);

            let (selected, members) =
                select_persisted_migration_format(&formats, errors, &reference, drive_count, Some(DiskError::FaultyDisk))
                    .expect("a persisted strict majority must outrank a nonmember read error")
                    .expect("the persisted formats should be compatible");

            assert_eq!(selected.shared_identity(), reference.shared_identity());
            assert_eq!(members.iter().filter(|member| **member).count(), required);
            assert!(members[..required].iter().all(|member| *member));
            assert!(members[required..].iter().all(|member| !*member));
        }
    }

    #[tokio::test]
    async fn existing_format_load_excludes_a_malformed_outlier() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let mut majority = FormatV3::new(1, 3);
        let mut expected = majority.clone();
        expected.erasure.this = Uuid::nil();

        for (index, disk) in disks.iter().take(2).enumerate() {
            majority.erasure.this = majority.erasure.sets[0][index];
            save_format_file(disk, &Some(majority.clone()))
                .await
                .expect("majority format should be written");
        }
        let mut malformed = majority;
        malformed.erasure.sets[0][1] = malformed.erasure.sets[0][0];
        malformed.erasure.this = malformed.erasure.sets[0][2];
        save_format_file(&disks[2], &Some(malformed))
            .await
            .expect("malformed outlier should be written");

        assert_eq!(
            connect_load_init_formats(true, &mut disks, 1, 3, None)
                .await
                .expect("one malformed outlier must not block a valid strict majority"),
            expected
        );
        assert!(disks[0].is_some() && disks[1].is_some());
        assert!(disks[2].is_none(), "the malformed outlier must be isolated");
    }

    #[tokio::test]
    async fn fresh_format_load_does_not_initialize_with_a_missing_disk() {
        let (_temp_dir, mut disks) = two_local_disks_with_missing_third().await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::FirstDiskWait)
        ));
        assert!(matches!(
            connect_load_init_formats(false, &mut disks, 1, 3, None).await,
            Err(Error::NotFirstDisk)
        ));

        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none));
        assert!(matches!(
            errors.as_slice(),
            [
                Some(DiskError::UnformattedDisk),
                Some(DiskError::UnformattedDisk),
                Some(DiskError::DiskNotFound)
            ]
        ));
    }

    #[tokio::test]
    async fn compatible_legacy_format_migrates() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let legacy = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &legacy).await;

        let mut expected = legacy;
        expected.erasure.this = Uuid::nil();
        assert_eq!(
            connect_load_init_formats(true, &mut disks, 1, 3, None)
                .await
                .expect("compatible legacy format should migrate"),
            expected
        );
        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(
            errors.iter().all(Option::is_none),
            "every available disk should receive the migrated format"
        );
        for (index, format) in formats.iter().enumerate() {
            assert_eq!(
                format.as_ref().map(|format| format.erasure.this),
                Some(expected.erasure.sets[0][index]),
                "the migrated format must preserve each disk's physical slot"
            );
        }
    }

    #[tokio::test]
    async fn compatible_legacy_format_migrates_when_the_file_is_missing() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let legacy = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &legacy).await;
        disks[2]
            .as_ref()
            .expect("third legacy disk should exist")
            .make_volume(MIGRATING_META_BUCKET)
            .await
            .expect("legacy metadata volume should be created without a format file");

        connect_load_init_formats(true, &mut disks, 1, 3, None)
            .await
            .expect("a missing legacy format file should be initialized from the majority");
        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(
            errors.iter().all(Option::is_none),
            "every available disk should receive the migrated format"
        );
        assert_eq!(
            formats[2].as_ref().map(|format| format.erasure.this),
            Some(legacy.erasure.sets[0][2]),
            "the missing legacy format file must be replaced with the third slot"
        );
    }

    #[tokio::test]
    async fn legacy_format_migration_resumes_partial_rustfs_writes() {
        for drive_count in [3, 4] {
            let (_temp_dir, mut disks) = local_disks(drive_count).await;
            let legacy = FormatV3::new(1, drive_count);
            write_legacy_majority(&disks, &legacy).await;
            for (index, disk) in disks.iter().enumerate().take(drive_count / 2) {
                let mut disk_format = legacy.clone();
                disk_format.erasure.this = legacy.erasure.sets[0][index];
                save_format_file(disk, &Some(disk_format))
                    .await
                    .expect("partial RustFS format should be written");
            }

            let mut expected = legacy;
            expected.erasure.this = Uuid::nil();
            assert_eq!(
                connect_load_init_formats(true, &mut disks, 1, drive_count, None)
                    .await
                    .expect("a partial migration should resume from the legacy quorum"),
                expected
            );
            let (formats, errors) = load_format_erasure_all(&disks, false).await;
            assert!(errors.iter().all(Option::is_none), "the resumed migration should format every disk");
            for (index, format) in formats.iter().enumerate() {
                assert_eq!(
                    format.as_ref().map(|format| format.erasure.this),
                    Some(expected.erasure.sets[0][index]),
                    "the resumed migration must preserve each physical slot"
                );
            }
        }
    }

    #[tokio::test]
    async fn legacy_format_migration_resumes_to_quorum_with_an_unavailable_disk() {
        let (_temp_dir, mut disks) = two_local_disks_with_missing_third().await;
        let legacy = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &legacy).await;
        let mut partial = legacy.clone();
        partial.erasure.this = partial.erasure.sets[0][0];
        save_format_file(&disks[0], &Some(partial))
            .await
            .expect("partial RustFS format should be written");

        let mut expected = legacy;
        expected.erasure.this = Uuid::nil();
        assert_eq!(
            connect_load_init_formats(true, &mut disks, 1, 3, None)
                .await
                .expect("the available blank disk should complete the migration quorum"),
            expected
        );
        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(formats[0].is_some() && formats[1].is_some());
        assert!(formats[2].is_none());
        assert!(matches!(errors[2], Some(DiskError::DiskNotFound)));
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_a_foreign_partial_rustfs_format() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let legacy = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &legacy).await;
        let mut foreign = FormatV3::new(1, 3);
        foreign.erasure.this = foreign.erasure.sets[0][0];
        let foreign_id = foreign.id;
        save_format_file(&disks[0], &Some(foreign))
            .await
            .expect("foreign partial RustFS format should be written");

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert_eq!(formats[0].as_ref().map(|format| format.id), Some(foreign_id));
        assert!(formats[1..].iter().all(Option::is_none), "foreign partial state must not be extended");
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_a_wrong_slot_partial_rustfs_format() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let legacy = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &legacy).await;
        let mut wrong_slot = legacy;
        wrong_slot.erasure.this = wrong_slot.erasure.sets[0][1];
        save_format_file(&disks[0], &Some(wrong_slot))
            .await
            .expect("wrong-slot partial RustFS format should be written");

        let error = connect_load_init_formats(true, &mut disks, 1, 3, None)
            .await
            .expect_err("a wrong-slot partial RustFS format must be rejected");
        assert!(matches!(error, Error::CorruptedFormat), "unexpected error: {error:?}");
        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(matches!(errors[0], Some(DiskError::InconsistentDisk)));
        assert!(formats[1..].iter().all(Option::is_none), "wrong-slot partial state must not be extended");
    }

    #[tokio::test]
    async fn legacy_format_migration_does_not_extend_a_malformed_rustfs_format() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let legacy = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &legacy).await;
        write_rustfs_bytes(&disks[0], bytes::Bytes::from_static(b"{not-json")).await;

        assert!(connect_load_init_formats(true, &mut disks, 1, 3, None).await.is_err());
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "malformed partial state must not be extended");
    }

    #[tokio::test]
    async fn incompatible_legacy_format_fails_closed() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let mut legacy = FormatV3::new(1, 3);
        legacy.id = Uuid::nil();
        write_legacy_majority(&disks, &legacy).await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));

        let (formats, errors) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "incompatible legacy metadata must not be replaced");
        assert!(
            errors.iter().all(|error| matches!(error, Some(DiskError::UnformattedDisk))),
            "fresh RustFS formats must not be written after a rejected migration: {errors:?}"
        );
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_a_foreign_minority() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let majority = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &majority).await;
        let minority = FormatV3::new(1, 3);
        let mut minority_disk = minority;
        minority_disk.erasure.this = minority_disk.erasure.sets[0][2];
        write_legacy_format(&disks[2], &minority_disk).await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "a foreign legacy minority must not be overwritten");
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_a_wrong_slot_minority() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let majority = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &majority).await;
        let mut wrong_slot = majority;
        wrong_slot.erasure.this = wrong_slot.erasure.sets[0][1];
        write_legacy_format(&disks[2], &wrong_slot).await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(
            formats.iter().all(Option::is_none),
            "a wrong-slot legacy minority must not be overwritten"
        );
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_a_malformed_minority() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let majority = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &majority).await;
        write_legacy_bytes(&disks[2], bytes::Bytes::from_static(b"{not-json")).await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "a malformed legacy minority must not be overwritten");
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_an_empty_minority() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let majority = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &majority).await;
        write_legacy_bytes(&disks[2], bytes::Bytes::new()).await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "an empty legacy minority must not be overwritten");
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_a_parseable_invalid_minority() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        let majority = FormatV3::new(1, 3);
        write_legacy_majority(&disks, &majority).await;
        let mut invalid = majority;
        invalid.id = Uuid::nil();
        invalid.erasure.this = invalid.erasure.sets[0][2];
        write_legacy_format(&disks[2], &invalid).await;

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(
            formats.iter().all(Option::is_none),
            "a parseable invalid legacy minority must not be overwritten"
        );
    }

    #[tokio::test]
    async fn legacy_format_migration_rejects_conflicting_candidates_without_a_quorum() {
        let (_temp_dir, mut disks) = local_disks(3).await;
        for (index, disk) in disks.iter().enumerate().take(2) {
            let mut disk_format = FormatV3::new(1, 3);
            disk_format.erasure.this = disk_format.erasure.sets[0][index];
            write_legacy_format(disk, &disk_format).await;
        }

        assert!(matches!(
            connect_load_init_formats(true, &mut disks, 1, 3, None).await,
            Err(Error::CorruptedFormat)
        ));
        let (formats, _) = load_format_erasure_all(&disks, false).await;
        assert!(formats.iter().all(Option::is_none), "a legacy split vote must not write RustFS formats");
    }
}

// #[derive(Debug, PartialEq, thiserror::Error)]
// pub enum ErasureError {
//     #[error("erasure read quorum")]
//     ErasureReadQuorum,

//     #[error("erasure write quorum")]
//     _ErasureWriteQuorum,

//     #[error("not first disk")]
//     NotFirstDisk,

//     #[error("first disk wait")]
//     FirstDiskWait,

//     #[error("invalid part id {0}")]
//     InvalidPart(usize),
// }

// impl ErasureError {
//     pub fn is(&self, err: &Error) -> bool {
//         if let Some(e) = err.downcast_ref::<ErasureError>() {
//             return self == e;
//         }

//         false
//     }
// }

// impl ErasureError {
//     pub fn to_u32(&self) -> u32 {
//         match self {
//             ErasureError::ErasureReadQuorum => 0x01,
//             ErasureError::_ErasureWriteQuorum => 0x02,
//             ErasureError::NotFirstDisk => 0x03,
//             ErasureError::FirstDiskWait => 0x04,
//             ErasureError::InvalidPart(_) => 0x05,
//         }
//     }

//     pub fn from_u32(error: u32) -> Option<Self> {
//         match error {
//             0x01 => Some(ErasureError::ErasureReadQuorum),
//             0x02 => Some(ErasureError::_ErasureWriteQuorum),
//             0x03 => Some(ErasureError::NotFirstDisk),
//             0x04 => Some(ErasureError::FirstDiskWait),
//             0x05 => Some(ErasureError::InvalidPart(Default::default())),
//             _ => None,
//         }
//     }
// }
