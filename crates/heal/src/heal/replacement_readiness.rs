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

use std::{fs, path::Path};

#[cfg(test)]
use super::Endpoint;
use super::{DiskStore, HealDiskExt as _, local_disk_map_read, resume::ReplacementTargetIdentity};

pub(crate) async fn auto_replacement_target_ready(disk: &DiskStore, local_disks: &[DiskStore]) -> bool {
    auto_replacement_target_identity(disk, local_disks).await.is_some()
}

pub(crate) async fn auto_replacement_target_identity(
    disk: &DiskStore,
    local_disks: &[DiskStore],
) -> Option<ReplacementTargetIdentity> {
    let lease_root = disk.replacement_mount_lease_root()?;
    let endpoint = disk.endpoint().to_string();
    let sibling_lease_roots = local_disks
        .iter()
        .filter(|sibling| sibling.endpoint().is_local && sibling.endpoint().to_string() != endpoint)
        .map(|sibling| sibling.replacement_mount_lease_root())
        .collect::<Option<Vec<_>>>()?;
    tokio::task::spawn_blocking(move || {
        let canonical_path = fs::canonicalize(&lease_root).ok()?;
        let metadata = fs::metadata(&lease_root).ok()?;
        let Ok(target_device_ids) = rustfs_utils::os::get_physical_device_ids(lease_root.to_string_lossy().as_ref()) else {
            return None;
        };
        let Ok(root_device_ids) = rustfs_utils::os::get_physical_device_ids("/") else {
            return None;
        };
        if target_device_ids.is_empty()
            || root_device_ids.is_empty()
            || target_device_ids.iter().any(|target| root_device_ids.contains(target))
            || !rustfs_utils::os::is_mount_point(&canonical_path).unwrap_or(false)
        {
            return None;
        }

        if sibling_lease_roots.iter().any(|sibling_lease_root| {
            rustfs_utils::os::get_physical_device_ids(sibling_lease_root.to_string_lossy().as_ref())
                .map(|ids| ids.iter().any(|id| target_device_ids.contains(id)))
                .unwrap_or(true)
        }) {
            return None;
        }

        let filesystem_identity = filesystem_identity(&metadata, &canonical_path)?;

        Some(ReplacementTargetIdentity {
            endpoint,
            canonical_path: canonical_path.to_string_lossy().into_owned(),
            physical_device_ids: target_device_ids,
            filesystem_identity,
        })
    })
    .await
    .ok()
    .flatten()
}

pub(crate) async fn auto_replacement_targets_ready(targets: &[String]) -> bool {
    auto_replacement_target_identities(targets).await.is_some()
}

pub(crate) async fn auto_replacement_target_identities(targets: &[String]) -> Option<Vec<ReplacementTargetIdentity>> {
    let local_disk_map = local_disk_map_read().await;
    let local_disks = local_disk_map
        .values()
        .flatten()
        .filter(|disk| disk.endpoint().is_local)
        .cloned()
        .collect::<Vec<_>>();
    drop(local_disk_map);

    let mut identities = Vec::with_capacity(targets.len());
    for target in targets {
        let disk = local_disks.iter().find(|disk| disk.endpoint().to_string() == *target)?;
        identities.push(auto_replacement_target_identity(disk, &local_disks).await?);
    }
    identities.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    identities.dedup_by(|left, right| left.endpoint == right.endpoint);
    (identities.len() == targets.len()).then_some(identities)
}

#[cfg(target_os = "linux")]
fn filesystem_identity(metadata: &fs::Metadata, canonical_path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    let escaped_path = canonical_path.to_string_lossy().replace(' ', "\\040");
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mount_id = mountinfo.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let mount_id = fields.next()?;
        fields.next()?;
        fields.next()?;
        fields.next()?;
        (fields.next()? == escaped_path).then_some(mount_id)
    })?;
    Some(format!("{mount_id}:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn filesystem_identity(metadata: &fs::Metadata, _canonical_path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    Some(format!("{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn filesystem_identity(_metadata: &fs::Metadata, _canonical_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::super::{DiskOption, new_disk};
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn runtime_environment_cannot_bypass_mount_admission() {
        temp_env::async_with_vars(
            [
                ("RUSTFS_TEST_AUTO_REPLACEMENT_READINESS_BYPASS", Some("1")),
                ("RUSTFS_E2E_AUTO_REPLACEMENT_READINESS_BYPASS", Some("1")),
            ],
            async {
                let temp = TempDir::new().expect("temporary replacement root should be created");
                let endpoint =
                    Endpoint::try_from(temp.path().to_string_lossy().as_ref()).expect("replacement endpoint should parse");

                let disk = new_disk(
                    &endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .expect("temporary disk should initialize");
                assert!(!auto_replacement_target_ready(&disk, std::slice::from_ref(&disk)).await);
            },
        )
        .await;
    }
}
