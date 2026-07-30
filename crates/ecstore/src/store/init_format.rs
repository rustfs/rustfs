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
use crate::{
    disk::{
        DiskInfoOptions, DiskOption, DiskStore, FORMAT_CONFIG_FILE, MIGRATING_META_BUCKET, RUSTFS_META_BUCKET,
        error::DiskError,
        format::{FormatBackend, FormatErasureVersion, FormatMetaVersion, FormatV3},
        new_disk,
    },
    layout::endpoints::Endpoints,
};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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

pub async fn connect_load_init_formats(
    first_disk: bool,
    disks: &mut [Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3> {
    let (formats, errs) = load_format_erasure_all(disks, false).await;

    check_disk_fatal_errs(&errs)?;

    if first_disk && should_init_erasure_disks(&errs) {
        // UnformattedDisk, try migrate from MinIO format first, else create new format
        info!("first_disk && should_init_erasure_disks");
        match try_migrate_format(disks, set_count, set_drive_count).await {
            Ok(LegacyFormatOutcome::Migrated(fm)) => {
                info!("Migrated format from MinIO config");
                return Ok(*fm);
            }
            Ok(LegacyFormatOutcome::Incompatible) => {
                // A MinIO format.json was found on disk but could not be migrated
                // (topology/version mismatch or parse failure). Falling through to
                // create a FRESH RustFS format changes the object placement layout,
                // so the pre-existing MinIO objects will not be readable. Surface
                // this loudly instead of silently discarding the legacy data.
                error!(
                    "Detected MinIO format.json on disk but could NOT migrate it; initializing a fresh RustFS format instead. \
                     Existing MinIO objects will not be readable under the new format. Ensure the RustFS pool / erasure-set \
                     topology exactly matches the original MinIO deployment, and that no stale .rustfs.sys/format.json remains."
                );
            }
            Ok(LegacyFormatOutcome::None) => {}
            Err(e) => {
                warn!("MinIO format migration attempt failed, will initialize a fresh format: {e}");
            }
        }
        let fm = init_format_erasure(disks, set_count, set_drive_count, deployment_id).await?;
        return Ok(fm);
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

    let fm = get_format_erasure_in_quorum(&formats, 0)?;
    check_format_erasure_value_for_topology(&fm, formats.len(), set_drive_count)?;
    let quorum_key = fm.shared_identity();
    for (index, (disk, format)) in disks.iter_mut().zip(&formats).enumerate() {
        let belongs_to_quorum = format
            .as_ref()
            .is_some_and(|format| format_disk_id_matches_slot(format, index) && format.shared_identity() == quorum_key);
        if !belongs_to_quorum {
            *disk = None;
        }
    }

    Ok(fm)
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
    disks: &[Option<DiskStore>],
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

    save_format_file_all(disks, &fms).await?;

    get_format_erasure_in_quorum(&fms, 0)
}

/// Outcome of attempting to migrate an on-disk MinIO `format.json`.
enum LegacyFormatOutcome {
    /// A compatible MinIO format was found and migrated into RustFS format files.
    /// Boxed to keep the enum small (`FormatV3` is large; the others are unit variants).
    Migrated(Box<FormatV3>),
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
/// and, if so, whether it was compatible. `Err` is only returned for genuine IO
/// failures while persisting the migrated format.
async fn try_migrate_format(
    disks: &[Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
) -> Result<LegacyFormatOutcome> {
    let mut legacy_seen = false;

    for disk in disks.iter().flatten() {
        let data = match disk.read_all(MIGRATING_META_BUCKET, FORMAT_CONFIG_FILE).await {
            Ok(d) if !d.is_empty() => d,
            _ => continue,
        };
        // A non-empty MinIO format.json exists on at least one disk.
        legacy_seen = true;

        let fm = match FormatV3::try_from(data.as_ref()) {
            Ok(fm) => fm,
            Err(e) => {
                warn!("failed to parse MinIO format.json, skipping this disk: {e}");
                continue;
            }
        };

        let Some(first_set) = fm.erasure.sets.first() else {
            warn!("MinIO format.json has empty erasure.sets, skipping this disk");
            continue;
        };
        if fm.erasure.sets.len() != set_count || first_set.len() != set_drive_count {
            warn!(
                "MinIO format topology mismatch: got {}x{}, expected {}x{}; skipping migration for this disk",
                fm.erasure.sets.len(),
                first_set.len(),
                set_count,
                set_drive_count
            );
            continue;
        }

        if fm.erasure.version != FormatErasureVersion::V3 {
            warn!(
                "MinIO format erasure version is not V3 ({:?}); skipping migration for this disk",
                fm.erasure.version
            );
            continue;
        }

        let mut fms = vec![None; disks.len()];
        for (idx, disk_opt) in disks.iter().enumerate() {
            if disk_opt.is_none() {
                continue;
            }
            let set_idx = idx / set_drive_count;
            let disk_idx = idx % set_drive_count;
            if set_idx >= fm.erasure.sets.len() || disk_idx >= fm.erasure.sets[set_idx].len() {
                continue;
            }
            let mut newfm = fm.clone();
            newfm.erasure.this = fm.erasure.sets[set_idx][disk_idx];
            fms[idx] = Some(newfm);
        }

        save_format_file_all(disks, &fms).await?;
        return Ok(LegacyFormatOutcome::Migrated(Box::new(get_format_erasure_in_quorum(&fms, 0)?)));
    }

    Ok(if legacy_seen {
        LegacyFormatOutcome::Incompatible
    } else {
        LegacyFormatOutcome::None
    })
}

pub(crate) fn format_disk_id_matches_slot(format: &FormatV3, index: usize) -> bool {
    let Ok(set_drive_count) = validate_format_erasure_layout(format) else {
        return false;
    };
    format
        .erasure
        .sets
        .get(index / set_drive_count)
        .and_then(|set| set.get(index % set_drive_count))
        .is_some_and(|expected| *expected == format.erasure.this)
}

pub fn get_format_erasure_in_quorum(formats: &[Option<FormatV3>], slot_offset: usize) -> Result<FormatV3> {
    let mut candidates = HashMap::new();
    let formats_present = formats.iter().flatten().count();
    let required_votes = formats.len() / 2 + 1;

    for format in formats.iter().enumerate().filter_map(|(index, format)| {
        let format = format.as_ref()?;
        let slot = slot_offset.checked_add(index)?;
        format_disk_id_matches_slot(format, slot).then_some(format)
    }) {
        let key = format.shared_identity();
        candidates
            .entry(key)
            .and_modify(|(_, count)| *count += 1)
            .or_insert((format, 1));
    }

    let candidate_groups = candidates.len();
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
    let Some((format, max_count)) = candidates.into_values().max_by_key(|(_, count)| *count) else {
        log_quorum_failure(0);
        return Err(Error::ErasureReadQuorum);
    };

    if max_count < required_votes {
        log_quorum_failure(max_count);
        return Err(Error::ErasureReadQuorum);
    }

    let mut format = (*format).clone();
    format.erasure.this = Uuid::nil();
    format.disk_info = None;

    Ok(format)
}

pub fn check_format_erasure_values(
    formats: &[Option<FormatV3>],
    // disks: &Vec<Option<DiskStore>>,
    set_drive_count: usize,
) -> Result<()> {
    for format in formats.iter().flatten() {
        check_format_erasure_value_for_topology(format, formats.len(), set_drive_count)?;
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
    let mut disk_ids = HashSet::new();

    for set in &format.erasure.sets {
        if set.len() != set_drive_count {
            return Err(Error::other("erasure.sets must be rectangular"));
        }
        for disk_id in set {
            if disk_id.is_nil() || *disk_id == Uuid::max() {
                return Err(Error::other("erasure.sets contains an invalid disk UUID"));
            }
            if !disk_ids.insert(*disk_id) {
                return Err(Error::other("erasure.sets contains a duplicate disk UUID"));
            }
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
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(s) => {
                if !heal {
                    let _ = disks[i].as_ref().unwrap().set_disk_id(Some(s.erasure.this)).await;
                }

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

async fn save_format_file_all(disks: &[Option<DiskStore>], formats: &[Option<FormatV3>]) -> disk::error::Result<()> {
    let mut futures = Vec::with_capacity(disks.len());

    for (i, disk) in disks.iter().enumerate() {
        futures.push(save_format_file(disk, &formats[i]));
    }

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

    let json_data = format.to_json()?;

    let tmpfile = Uuid::new_v4().to_string();

    disk.write_all(RUSTFS_META_BUCKET, tmpfile.as_str(), json_data.into_bytes().into())
        .await?;

    disk.rename_file(RUSTFS_META_BUCKET, tmpfile.as_str(), RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
        .await?;

    disk.set_disk_id(Some(format.erasure.this)).await?;

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
