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
use crate::disk::health_state::DriveMembershipSnapshot;

impl SetDisks {
    pub(super) fn format_lock_error(&self, bucket: &str, object: &str, mode: &str, err: &LockResult) -> String {
        match err {
            LockResult::Timeout => {
                format!("{mode} lock acquisition timed out on {bucket}/{object} (owner={})", self.locker_owner)
            }
            LockResult::Conflict {
                current_owner,
                current_mode,
            } => format!("{mode} lock conflicted on {bucket}/{object}: held by {current_owner} as {current_mode:?}"),
            LockResult::Acquired => format!("unexpected lock state while acquiring {mode} lock on {bucket}/{object}"),
        }
    }

    pub(super) fn format_lock_error_from_error(
        &self,
        bucket: &str,
        object: &str,
        mode: &str,
        err: &rustfs_lock::error::LockError,
    ) -> String {
        match err {
            rustfs_lock::error::LockError::Timeout { .. } => {
                format!(
                    "ns_loc: {mode} lock acquisition timed out on {bucket}/{object} (owner={})",
                    self.locker_owner
                )
            }
            rustfs_lock::error::LockError::AlreadyLocked { owner, .. } => {
                format!("ns_loc: {mode} lock conflicted on {bucket}/{object}: held by {owner}")
            }
            _ => format!("ns_loc: {mode} lock acquisition failed on {bucket}/{object}: {}", err),
        }
    }

    pub(super) async fn get_disks_internal(&self) -> Vec<Option<DiskStore>> {
        let rl = self.disks.read().await;

        rl.clone()
    }

    pub async fn get_local_disks(&self) -> Vec<Option<DiskStore>> {
        let rl = self.disks.read().await;

        let mut disks: Vec<Option<DiskStore>> = rl
            .clone()
            .into_iter()
            .filter(|v| v.as_ref().is_some_and(|d| d.is_local()))
            .collect();

        let mut rng = rand::rng();

        disks.shuffle(&mut rng);

        disks
    }

    pub(super) async fn get_online_disks(&self) -> Vec<Option<DiskStore>> {
        let snapshot = self.drive_membership_snapshot().await;
        let mut disks = snapshot.strict_online_candidates().into_iter().map(Some).collect::<Vec<_>>();

        let mut rng = rand::rng();
        disks.shuffle(&mut rng);

        disks
    }

    pub(super) async fn get_online_local_disks(&self) -> Vec<Option<DiskStore>> {
        let snapshot = self.drive_membership_snapshot().await;
        let mut disks = snapshot
            .strict_online_local_candidates()
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();

        let mut rng = rand::rng();

        disks.shuffle(&mut rng);

        disks
    }

    pub async fn get_online_disks_with_healing(&self, incl_healing: bool) -> (Vec<DiskStore>, bool) {
        let (disks, _, healing) = self.get_online_disks_with_healing_and_info(incl_healing).await;
        (disks, healing > 0)
    }

    pub async fn drive_membership_snapshot(&self) -> DriveMembershipSnapshot {
        let disks = self.get_disks_internal().await;
        DriveMembershipSnapshot::from_optional_disks(&disks)
    }

    async fn reprobe_runtime_candidates_once(&self, disks: &[DiskStore]) {
        for disk in disks {
            if disk.runtime_state() != crate::disk::health_state::RuntimeDriveHealthState::Online {
                disk.reset_health_for_store_init_retry();
            }
        }
    }

    pub async fn get_online_disks_with_healing_and_info(&self, incl_healing: bool) -> (Vec<DiskStore>, Vec<DiskInfo>, usize) {
        let snapshot = self.drive_membership_snapshot().await;
        let mut membership_candidates = snapshot.scanner_heal_candidates();
        let mut reprobed = false;

        loop {
            let mut disks = membership_candidates.iter().cloned().map(Some).collect::<Vec<_>>();
            let mut infos: Vec<Option<DiskInfo>> = vec![None; disks.len()];

            let mut futures = Vec::with_capacity(disks.len());
            {
                let mut rng = rand::rng();
                disks.shuffle(&mut rng);
            }

            for (i, disk) in disks.iter().cloned().enumerate() {
                futures.push(async move {
                    let info = if let Some(disk) = disk {
                        match disk.disk_info(&DiskInfoOptions::default()).await {
                            Ok(info) => info,
                            Err(err) => DiskInfo {
                                error: err.to_string(),
                                ..Default::default()
                            },
                        }
                    } else {
                        DiskInfo {
                            error: DiskError::DiskNotFound.to_string(),
                            ..Default::default()
                        }
                    };

                    Ok((i, info))
                });
            }

            let processor = get_global_processors().metadata_processor();
            let results = processor.execute_batch(futures).await;

            for (submitted_idx, result) in results.into_iter().enumerate() {
                match result {
                    Ok((disk_idx, info)) => {
                        infos[disk_idx] = Some(info);
                    }
                    Err(err) => {
                        infos[submitted_idx] = Some(DiskInfo {
                            error: err.to_string(),
                            ..Default::default()
                        });
                    }
                }
            }

            let mut healing: usize = 0;

            let mut scanning_disks = Vec::new();
            let mut healing_disks = Vec::new();
            let mut scanning_infos = Vec::new();
            let mut healing_infos = Vec::new();

            let mut new_disks = Vec::new();
            let mut new_infos = Vec::new();

            for (disk, info) in disks.into_iter().zip(infos) {
                let Some(info) = info else {
                    continue;
                };

                if !info.error.is_empty() || disk.is_none() {
                    continue;
                }

                if info.healing {
                    healing += 1;
                    if incl_healing {
                        healing_disks.push(disk.expect("disk should exist when info succeeded"));
                        healing_infos.push(info);
                    }

                    continue;
                }

                if !info.scanning {
                    new_disks.push(disk.expect("disk should exist when info succeeded"));
                    new_infos.push(info);
                } else {
                    scanning_disks.push(disk.expect("disk should exist when info succeeded"));
                    scanning_infos.push(info);
                }
            }

            new_disks.extend(scanning_disks);
            new_infos.extend(scanning_infos);
            new_disks.extend(healing_disks);
            new_infos.extend(healing_infos);

            if !new_disks.is_empty() || membership_candidates.is_empty() || reprobed {
                return (new_disks, new_infos, healing);
            }

            reprobed = true;
            self.reprobe_runtime_candidates_once(&membership_candidates).await;
            membership_candidates = self.drive_membership_snapshot().await.scanner_heal_candidates();
        }
    }

    pub(super) async fn _get_local_disks(&self) -> Vec<Option<DiskStore>> {
        let mut disks = self.get_disks_internal().await;

        let mut rng = rand::rng();

        disks.shuffle(&mut rng);

        disks
            .into_iter()
            .filter(|v| v.as_ref().is_some_and(|d| d.is_local()))
            .collect()
    }

    pub async fn connect_disks(&self) {
        let rl = self.disks.read().await;

        let disks = rl.clone();

        // Explicitly release the lock
        drop(rl);

        for (i, opdisk) in disks.iter().enumerate() {
            if let Some(disk) = opdisk {
                if disk.is_online().await && disk.get_disk_location().set_idx.is_some() {
                    info!("Disk {:?} is online", disk.to_string());
                    continue;
                }

                let _ = disk.close().await;
            }

            if let Some(endpoint) = self.set_endpoints.get(i) {
                info!("will renew disk, opdisk: {:?}", opdisk);
                self.renew_disk(endpoint).await;
            }
        }
    }

    pub async fn renew_disk(&self, ep: &Endpoint) {
        debug!("renew_disk: start {:?}", ep);

        let (new_disk, fm) = match Self::connect_endpoint(ep).await {
            Ok(res) => res,
            Err(e) => {
                warn!("renew_disk: connect_endpoint err {:?}", &e);
                if ep.is_local && e == DiskError::UnformattedDisk {
                    info!("renew_disk unformatteddisk will trigger heal_disk, {:?}", ep);
                    let set_disk_id = format!("pool_{}_set_{}", ep.pool_idx, ep.set_idx);
                    let _ = send_heal_disk(set_disk_id, Some(HealChannelPriority::Normal)).await;
                }
                return;
            }
        };

        let (set_idx, disk_idx) = match self.find_disk_index(&fm) {
            Ok(res) => res,
            Err(e) => {
                warn!("renew_disk: find_disk_index err {:?}", e);
                return;
            }
        };

        // Check that the endpoint matches

        let _ = new_disk.set_disk_id(Some(fm.erasure.this)).await;

        if new_disk.is_local() {
            let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;
            let path = new_disk.endpoint().to_string();
            global_local_disk_map.insert(path, Some(new_disk.clone()));

            if is_dist_erasure().await {
                let mut local_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.write().await;
                local_set_drives[self.pool_index][set_idx][disk_idx] = Some(new_disk.clone());
            }
        }

        debug!("renew_disk: update {:?}", fm.erasure.this);

        let mut disk_lock = self.disks.write().await;
        disk_lock[disk_idx] = Some(new_disk);
    }

    pub(super) fn find_disk_index(&self, fm: &FormatV3) -> Result<(usize, usize)> {
        self.format.check_other(fm)?;

        if fm.erasure.this.is_nil() {
            return Err(Error::other("DriveID: offline"));
        }

        for i in 0..self.format.erasure.sets.len() {
            for j in 0..self.format.erasure.sets[0].len() {
                if fm.erasure.this == self.format.erasure.sets[i][j] {
                    return Ok((i, j));
                }
            }
        }

        Err(Error::other("DriveID: not found"))
    }

    pub(super) async fn connect_endpoint(ep: &Endpoint) -> disk::error::Result<(DiskStore, FormatV3)> {
        let disk = new_disk(ep, &DiskOption::default()).await?;

        let fm = load_format_erasure(&disk, false).await?;

        Ok((disk, fm))
    }

    pub(super) async fn get_online_disk_with_healing(&self, incl_healing: bool) -> Result<(Vec<Option<DiskStore>>, bool)> {
        let (new_disks, _, healing) = self.get_online_disk_with_healing_and_info(incl_healing).await?;
        Ok((new_disks, healing > 0))
    }

    pub(super) async fn get_online_disk_with_healing_and_info(
        &self,
        incl_healing: bool,
    ) -> Result<(Vec<Option<DiskStore>>, Vec<DiskInfo>, usize)> {
        let mut infos = vec![DiskInfo::default(); self.disks.read().await.len()];
        for (idx, disk) in self.disks.write().await.iter().enumerate() {
            if let Some(disk) = disk {
                match disk.disk_info(&DiskInfoOptions::default()).await {
                    Ok(disk_info) => infos[idx] = disk_info,
                    Err(err) => infos[idx].error = err.to_string(),
                }
            } else {
                infos[idx].error = "disk not found".to_string();
            }
        }

        let mut new_disks = Vec::new();
        let mut healing_disks = Vec::new();
        let mut scanning_disks = Vec::new();
        let mut new_infos = Vec::new();
        let mut healing_infos = Vec::new();
        let mut scanning_infos = Vec::new();
        let mut healing = 0;

        infos.iter().zip(self.disks.write().await.iter()).for_each(|(info, disk)| {
            if info.error.is_empty() {
                if info.healing {
                    healing += 1;
                    if incl_healing {
                        healing_disks.push(disk.clone());
                        healing_infos.push(info.clone());
                    }
                } else if !info.scanning {
                    new_disks.push(disk.clone());
                    new_infos.push(info.clone());
                } else {
                    scanning_disks.push(disk.clone());
                    scanning_infos.push(info.clone());
                }
            }
        });

        // Prefer non-scanning disks over disks which are currently being scanned.
        new_disks.extend(scanning_disks);
        new_infos.extend(scanning_infos);

        // Then add healing disks.
        new_disks.extend(healing_disks);
        new_infos.extend(healing_infos);

        Ok((new_disks, new_infos, healing))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_init::save_format_file;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    async fn make_formatted_local_disk(disk_idx: usize, format: &FormatV3) -> (TempDir, Endpoint, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_idx);

        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("local disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][disk_idx];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        (dir, endpoint, disk)
    }

    #[tokio::test]
    async fn get_online_disks_with_healing_and_info_keeps_disk_and_info_aligned() {
        let disk_count = 8;
        let format = FormatV3::new(1, disk_count);

        let mut temp_dirs = Vec::with_capacity(disk_count);
        let mut endpoints = Vec::with_capacity(disk_count);
        let mut disks = Vec::with_capacity(disk_count);

        for disk_idx in 0..disk_count {
            let (temp_dir, endpoint, disk) = make_formatted_local_disk(disk_idx, &format).await;
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            disk_count,
            disk_count / 2,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
        )
        .await;

        for _ in 0..32 {
            let (online_disks, infos, healing) = set_disks.get_online_disks_with_healing_and_info(false).await;
            assert_eq!(healing, 0);
            assert_eq!(online_disks.len(), disk_count);
            assert_eq!(infos.len(), disk_count);

            for (disk, info) in online_disks.iter().zip(infos.iter()) {
                assert!(
                    info.error.is_empty(),
                    "unexpected disk_info error for {}: {}",
                    disk.endpoint(),
                    info.error
                );
                assert_eq!(info.endpoint, disk.endpoint().to_string());
                assert_eq!(
                    info.id,
                    disk.get_disk_id().await.expect("disk id lookup should succeed"),
                    "disk info should stay aligned with disk {}",
                    disk.endpoint()
                );
            }
        }

        drop(temp_dirs);
    }

    #[tokio::test]
    async fn drive_membership_snapshot_filters_offline_disks_from_candidates() {
        let disk_count = 4;
        let format = FormatV3::new(1, disk_count);

        let mut temp_dirs = Vec::with_capacity(disk_count);
        let mut endpoints = Vec::with_capacity(disk_count);
        let mut disks = Vec::with_capacity(disk_count);

        for disk_idx in 0..disk_count {
            let (temp_dir, endpoint, disk) = make_formatted_local_disk(disk_idx, &format).await;
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            disk_count,
            disk_count / 2,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
        )
        .await;

        let all_disks = set_disks.get_disks_internal().await;
        all_disks[1]
            .as_ref()
            .expect("disk 1 should exist")
            .force_runtime_state_for_test(crate::disk::health_state::RuntimeDriveHealthState::Suspect);
        all_disks[2]
            .as_ref()
            .expect("disk 2 should exist")
            .force_runtime_state_for_test(crate::disk::health_state::RuntimeDriveHealthState::Returning);
        all_disks[3]
            .as_ref()
            .expect("disk 3 should exist")
            .force_runtime_state_for_test(crate::disk::health_state::RuntimeDriveHealthState::Offline);

        let snapshot = set_disks.drive_membership_snapshot().await;
        assert_eq!(snapshot.online.len(), 1);
        assert_eq!(snapshot.suspect.len(), 1);
        assert_eq!(snapshot.returning.len(), 1);
        assert_eq!(snapshot.offline.len(), 1);
        assert_eq!(snapshot.scanner_heal_candidates().len(), 3);

        let strict_online = set_disks.get_online_disks().await;
        assert_eq!(strict_online.len(), 1, "strict online selection should exclude suspect/returning/offline");

        let (online_disks, infos, healing) = set_disks.get_online_disks_with_healing_and_info(false).await;
        assert_eq!(healing, 0);
        assert_eq!(online_disks.len(), 3);
        assert_eq!(infos.len(), 3);
        assert!(
            online_disks
                .iter()
                .all(|disk| { disk.runtime_state() != crate::disk::health_state::RuntimeDriveHealthState::Offline }),
            "offline disks should be filtered by membership snapshot"
        );

        drop(temp_dirs);
    }

    #[tokio::test]
    async fn get_online_disks_with_healing_and_info_reprobes_runtime_candidates_once() {
        let disk_count = 4;
        let format = FormatV3::new(1, disk_count);

        let mut temp_dirs = Vec::with_capacity(disk_count);
        let mut endpoints = Vec::with_capacity(disk_count);
        let mut disks = Vec::with_capacity(disk_count);

        for disk_idx in 0..disk_count {
            let (temp_dir, endpoint, disk) = make_formatted_local_disk(disk_idx, &format).await;
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            disk_count,
            disk_count / 2,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
        )
        .await;

        let all_disks = set_disks.get_disks_internal().await;
        for disk in all_disks.iter().flatten() {
            disk.force_runtime_state_for_test(crate::disk::health_state::RuntimeDriveHealthState::Returning);
        }

        let (online_disks, infos, healing) = set_disks.get_online_disks_with_healing_and_info(false).await;
        assert_eq!(healing, 0);
        assert_eq!(online_disks.len(), disk_count);
        assert_eq!(infos.len(), disk_count);
        assert!(
            infos.iter().all(|info| info.error.is_empty()),
            "runtime reprobe should recover a usable candidate set without probe errors"
        );

        drop(temp_dirs);
    }
}
