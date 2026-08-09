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

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::{Endpoint, HealDiskExt as _, local_disk_map_read, resume::ReplacementTargetIdentity};

pub(crate) async fn auto_replacement_target_ready(endpoint: &Endpoint, local_endpoints: &[Endpoint]) -> bool {
    auto_replacement_target_identity(endpoint, local_endpoints).await.is_some()
}

pub(crate) async fn auto_replacement_target_identity(
    endpoint: &Endpoint,
    local_endpoints: &[Endpoint],
) -> Option<ReplacementTargetIdentity> {
    if !endpoint.is_local {
        return None;
    }

    let path = PathBuf::from(endpoint.get_file_path());
    let sibling_paths = local_endpoints
        .iter()
        .filter(|sibling| sibling.is_local && *sibling != endpoint)
        .map(|sibling| sibling.get_file_path())
        .collect::<Vec<_>>();
    let endpoint = endpoint.to_string();
    tokio::task::spawn_blocking(move || {
        let path = path.to_string_lossy().into_owned();
        let canonical_path = fs::canonicalize(&path).ok()?;
        let metadata = fs::metadata(&canonical_path).ok()?;
        let Ok(target_device_ids) = rustfs_utils::os::get_physical_device_ids(path.as_ref()) else {
            return None;
        };
        let Ok(root_device_ids) = rustfs_utils::os::get_physical_device_ids("/") else {
            return None;
        };
        if target_device_ids.is_empty()
            || root_device_ids.is_empty()
            || target_device_ids.iter().any(|target| root_device_ids.contains(target))
            || !rustfs_utils::os::is_mount_point(Path::new(&path)).unwrap_or(false)
        {
            return None;
        }

        if sibling_paths.iter().any(|sibling| {
            rustfs_utils::os::get_physical_device_ids(sibling)
                .map(|ids| ids.iter().any(|id| target_device_ids.contains(id)))
                .unwrap_or(true)
        }) {
            return None;
        }

        Some(ReplacementTargetIdentity {
            endpoint,
            canonical_path: canonical_path.to_string_lossy().into_owned(),
            physical_device_ids: target_device_ids,
            filesystem_identity: filesystem_identity(&metadata, &canonical_path)?,
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
    let local_endpoints = local_disks.iter().map(|disk| disk.endpoint()).collect::<Vec<_>>();
    drop(local_disk_map);

    let mut identities = Vec::with_capacity(targets.len());
    for target in targets {
        let Some(disk) = local_disks.iter().find(|disk| disk.endpoint().to_string() == *target) else {
            return None;
        };
        if !disk.has_replacement_mount_lease() {
            return None;
        }
        let endpoint = disk.endpoint();
        identities.push(auto_replacement_target_identity(&endpoint, &local_endpoints).await?);
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
