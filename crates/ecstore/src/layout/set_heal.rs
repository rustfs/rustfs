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

use crate::disk::{DiskInfo, error::DiskError};
use crate::layout::{endpoints::Endpoints, format::FormatV3};
use rustfs_common::heal_channel::DriveState;
use rustfs_madmin::heal_commands::HealDriveInfo;

pub(crate) fn formats_to_drives_info(
    endpoints: &Endpoints,
    formats: &[Option<FormatV3>],
    errs: &[Option<DiskError>],
) -> Vec<HealDriveInfo> {
    let mut before_drives = Vec::with_capacity(endpoints.as_ref().len());
    for (index, format) in formats.iter().enumerate() {
        let drive = endpoints.get_string(index);
        let state = if format.is_some() {
            DriveState::Ok.to_string()
        } else if let Some(Some(err)) = errs.get(index) {
            if *err == DiskError::UnformattedDisk {
                DriveState::Missing.to_string()
            } else if *err == DiskError::DiskNotFound {
                DriveState::Offline.to_string()
            } else {
                DriveState::Corrupt.to_string()
            }
        } else {
            DriveState::Corrupt.to_string()
        };

        let uuid = if let Some(format) = format {
            format.erasure.this.to_string()
        } else {
            "".to_string()
        };
        before_drives.push(HealDriveInfo {
            uuid,
            endpoint: drive,
            state: state.to_string(),
        });
    }
    before_drives
}

pub(crate) fn new_heal_format_sets(
    ref_format: &FormatV3,
    set_count: usize,
    set_drive_count: usize,
    formats: &[Option<FormatV3>],
    errs: &[Option<DiskError>],
) -> (Vec<Vec<Option<FormatV3>>>, Vec<Vec<DiskInfo>>) {
    let mut new_formats = vec![vec![None; set_drive_count]; set_count];
    let mut current_disks_info = vec![vec![DiskInfo::default(); set_drive_count]; set_count];
    for (i, set) in ref_format.erasure.sets.iter().enumerate() {
        for j in 0..set.len() {
            if let Some(Some(err)) = errs.get(i * set_drive_count + j)
                && *err == DiskError::UnformattedDisk
            {
                let mut fm = FormatV3::new(set_count, set_drive_count);
                fm.id = ref_format.id;
                fm.format = ref_format.format.clone();
                fm.version = ref_format.version.clone();
                fm.erasure.this = ref_format.erasure.sets[i][j];
                fm.erasure.sets = ref_format.erasure.sets.clone();
                fm.erasure.version = ref_format.erasure.version.clone();
                fm.erasure.distribution_algo = ref_format.erasure.distribution_algo.clone();
                new_formats[i][j] = Some(fm);
            }
            if let (Some(format), None) = (&formats[i * set_drive_count + j], &errs[i * set_drive_count + j])
                && let Some(info) = &format.disk_info
                && !info.endpoint.is_empty()
            {
                current_disks_info[i][j] = info.clone();
            }
        }
    }

    (new_formats, current_disks_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoint::Endpoint;

    #[test]
    fn formats_to_drives_info_maps_missing_offline_and_ok_states() {
        let endpoints = Endpoints::from(vec![
            Endpoint::try_from("/tmp/rustfs-set-heal-a").unwrap(),
            Endpoint::try_from("/tmp/rustfs-set-heal-b").unwrap(),
            Endpoint::try_from("/tmp/rustfs-set-heal-c").unwrap(),
        ]);
        let format = FormatV3::new(1, 1);
        let uuid = format.erasure.this.to_string();
        let drives = formats_to_drives_info(
            &endpoints,
            &[Some(format), None, None],
            &[None, Some(DiskError::UnformattedDisk), Some(DiskError::DiskNotFound)],
        );

        assert_eq!(drives.len(), 3);
        assert_eq!(drives[0].uuid, uuid);
        assert_eq!(drives[0].state, DriveState::Ok.to_string());
        assert_eq!(drives[1].state, DriveState::Missing.to_string());
        assert_eq!(drives[2].state, DriveState::Offline.to_string());
    }

    #[test]
    fn new_heal_format_sets_only_recreates_unformatted_slots() {
        let ref_format = FormatV3::new(1, 2);
        let existing_format = ref_format.clone();
        let (new_formats, current_disks_info) = new_heal_format_sets(
            &ref_format,
            1,
            2,
            &[Some(existing_format), None],
            &[None, Some(DiskError::UnformattedDisk)],
        );

        assert!(new_formats[0][0].is_none());
        let repaired = new_formats[0][1].as_ref().unwrap();
        assert_eq!(repaired.id, ref_format.id);
        assert_eq!(repaired.erasure.this, ref_format.erasure.sets[0][1]);
        assert_eq!(repaired.erasure.sets, ref_format.erasure.sets);
        assert_eq!(current_disks_info.len(), 1);
        assert_eq!(current_disks_info[0].len(), 2);
    }
}
