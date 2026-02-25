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
        let mut disks = self.get_disks_internal().await;

        // TODO: diskinfo filter online

        let mut new_disk = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            if let Some(d) = disk
                && d.is_online().await
            {
                new_disk.push(disk.clone());
            }
        }

        let mut rng = rand::rng();

        disks.shuffle(&mut rng);

        new_disk
        // let disks = self.get_disks_internal().await;
        // let (filtered, _) = self.filter_online_disks(disks).await;
        // filtered.into_iter().filter(|disk| disk.is_some()).collect()
    }

    pub(super) async fn get_online_local_disks(&self) -> Vec<Option<DiskStore>> {
        let mut disks = self.get_online_disks().await;

        let mut rng = rand::rng();

        disks.shuffle(&mut rng);

        disks
            .into_iter()
            .filter(|v| v.as_ref().is_some_and(|d| d.is_local()))
            .collect()
    }

    pub async fn get_online_disks_with_healing(&self, incl_healing: bool) -> (Vec<DiskStore>, bool) {
        let (disks, _, healing) = self.get_online_disks_with_healing_and_info(incl_healing).await;
        (disks, healing > 0)
    }

    pub async fn get_online_disks_with_healing_and_info(&self, incl_healing: bool) -> (Vec<DiskStore>, Vec<DiskInfo>, usize) {
        let mut disks = self.get_disks_internal().await;

        let mut infos = Vec::with_capacity(disks.len());

        let mut futures = Vec::with_capacity(disks.len());
        let mut numbers: Vec<usize> = (0..disks.len()).collect();
        {
            let mut rng = rand::rng();
            disks.shuffle(&mut rng);

            numbers.shuffle(&mut rng);
        }

        for &i in numbers.iter() {
            let disk = disks[i].clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.disk_info(&DiskInfoOptions::default()).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        // Use optimized batch processor for disk info retrieval
        let processor = get_global_processors().metadata_processor();
        let results = processor.execute_batch(futures).await;

        for result in results {
            match result {
                Ok(res) => {
                    infos.push(res);
                }
                Err(err) => {
                    infos.push(DiskInfo {
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

        for &i in numbers.iter() {
            let (info, disk) = (infos[i].clone(), disks[i].clone());
            if !info.error.is_empty() || disk.is_none() {
                continue;
            }

            if info.healing {
                healing += 1;
                if incl_healing {
                    healing_disks.push(disk.unwrap());
                    healing_infos.push(info);
                }

                continue;
            }

            if !info.healing {
                new_disks.push(disk.unwrap());
                new_infos.push(info);
            } else {
                scanning_disks.push(disk.unwrap());
                scanning_infos.push(info);
            }
        }

        new_disks.extend(scanning_disks);
        new_infos.extend(scanning_infos);
        new_disks.extend(healing_disks);
        new_infos.extend(healing_infos);

        (new_disks, new_infos, healing)
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
