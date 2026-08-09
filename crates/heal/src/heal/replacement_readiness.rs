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

use std::path::{Path, PathBuf};

use super::{Endpoint, HealDiskExt as _, local_disk_map_read};

pub(crate) async fn auto_replacement_target_ready(endpoint: &Endpoint, local_endpoints: &[Endpoint]) -> bool {
    if !endpoint.is_local {
        return false;
    }

    let path = PathBuf::from(endpoint.get_file_path());
    let sibling_paths = local_endpoints
        .iter()
        .filter(|sibling| sibling.is_local && *sibling != endpoint)
        .map(|sibling| sibling.get_file_path())
        .collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let path = path.to_string_lossy();
        let Ok(target_device_ids) = rustfs_utils::os::get_physical_device_ids(path.as_ref()) else {
            return false;
        };
        let Ok(root_device_ids) = rustfs_utils::os::get_physical_device_ids("/") else {
            return false;
        };
        if target_device_ids.is_empty()
            || root_device_ids.is_empty()
            || target_device_ids.iter().any(|target| root_device_ids.contains(target))
            || !rustfs_utils::os::is_mount_point(Path::new(path.as_ref())).unwrap_or(false)
        {
            return false;
        }

        !sibling_paths.iter().any(|sibling| {
            rustfs_utils::os::get_physical_device_ids(sibling)
                .map(|ids| ids.iter().any(|id| target_device_ids.contains(id)))
                .unwrap_or(true)
        })
    })
    .await
    .unwrap_or(false)
}

pub(crate) async fn auto_replacement_targets_ready(targets: &[String]) -> bool {
    let local_disk_map = local_disk_map_read().await;
    let local_endpoints = local_disk_map
        .values()
        .flatten()
        .map(|disk| disk.endpoint())
        .filter(|endpoint| endpoint.is_local)
        .collect::<Vec<_>>();
    drop(local_disk_map);

    for target in targets {
        let Some(endpoint) = local_endpoints.iter().find(|endpoint| endpoint.to_string() == *target) else {
            return false;
        };
        if !auto_replacement_target_ready(endpoint, &local_endpoints).await {
            return false;
        }
    }
    true
}
