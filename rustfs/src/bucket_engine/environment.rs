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

//! This node's engine environment: identity, local drives and the placement
//! view, derived from the configured endpoints, plus the engine directories
//! the framework creates before an engine attaches and removes after a purge.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use rustfs_bucket_engine::{BucketContext, NodeEnvironment, NodeTargets, PlacementView};

use super::storage_api::get_global_endpoints_opt;

/// One configured drive, as the endpoint list describes it.
#[derive(Debug, Clone)]
pub(crate) struct EndpointDrive {
    /// The node's `host:port`.
    pub node: String,
    pub is_local: bool,
    pub path: PathBuf,
    /// Pool, set and disk index: the drive's order within its node.
    pub order: (i32, i32, i32),
}

/// Builds the environment from the configured drives. Nodes are ordered by
/// name and drives by (pool, set, disk), so every node derives the same view.
/// Every node starts online; liveness is refreshed separately. `None` when no
/// drive is local (no engine data can live on this process).
pub(crate) fn environment_from(drives: Vec<EndpointDrive>) -> Option<NodeEnvironment> {
    let mut nodes: BTreeMap<String, Vec<EndpointDrive>> = BTreeMap::new();
    for drive in drives {
        nodes.entry(drive.node.clone()).or_default().push(drive);
    }
    let mut local = None;
    let mut targets = Vec::with_capacity(nodes.len());
    for (node, mut drives) in nodes {
        drives.sort_by_key(|drive| drive.order);
        targets.push(NodeTargets {
            node: node.clone(),
            drives: u16::try_from(drives.len()).unwrap_or(u16::MAX),
            online: true,
        });
        if local.is_none() && drives.iter().any(|drive| drive.is_local) {
            local = Some((node, drives.into_iter().map(|drive| drive.path).collect::<Vec<_>>()));
        }
    }
    let (node, drives) = local?;
    Some(NodeEnvironment::new(node, drives, PlacementView::from_nodes(targets)))
}

/// The environment of this server from the global endpoint list.
pub(crate) fn environment_from_endpoints() -> Option<NodeEnvironment> {
    let pools = get_global_endpoints_opt()?;
    let mut drives = Vec::new();
    for pool in pools.as_ref() {
        for endpoint in pool.endpoints.as_ref() {
            drives.push(EndpointDrive {
                node: endpoint.host_port(),
                is_local: endpoint.is_local,
                path: PathBuf::from(endpoint.get_file_path()),
                order: (endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx),
            });
        }
    }
    environment_from(drives)
}

/// Creates the bucket incarnation's engine directory on every local drive.
pub(crate) async fn create_engine_dirs(environment: &NodeEnvironment, bucket: &BucketContext) -> io::Result<()> {
    for dir in environment.engine_dirs(&bucket.binding.engine, bucket.incarnation) {
        tokio::fs::create_dir_all(&dir).await?;
    }
    Ok(())
}

/// Removes the bucket incarnation's engine directory from every local drive;
/// missing directories are fine.
pub(crate) async fn remove_engine_dirs(environment: &NodeEnvironment, bucket: &BucketContext) -> io::Result<()> {
    for dir in environment.engine_dirs(&bucket.binding.engine, bucket.incarnation) {
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_bucket_engine::{BucketEngineBinding, EngineName};

    fn drive(node: &str, is_local: bool, path: &str, order: (i32, i32, i32)) -> EndpointDrive {
        EndpointDrive {
            node: node.to_owned(),
            is_local,
            path: PathBuf::from(path),
            order,
        }
    }

    fn layout(local: &str) -> Vec<EndpointDrive> {
        let mut drives = Vec::new();
        for node in ["10.0.0.2:9000", "10.0.0.1:9000"] {
            for disk in (0..4).rev() {
                drives.push(drive(node, node == local, &format!("/data/{node}/d{disk}"), (0, 0, disk)));
            }
        }
        drives
    }

    #[test]
    fn every_node_derives_the_same_view_and_its_own_drives() {
        let first = environment_from(layout("10.0.0.1:9000")).expect("local drives");
        let second = environment_from(layout("10.0.0.2:9000")).expect("local drives");

        assert_eq!(first.placement().version, second.placement().version);
        let nodes: Vec<_> = first.placement().nodes.iter().map(|node| node.node.clone()).collect();
        assert_eq!(nodes, ["10.0.0.1:9000", "10.0.0.2:9000"]);
        assert!(first.placement().nodes.iter().all(|node| node.drives == 4 && node.online));

        assert_eq!(first.node(), "10.0.0.1:9000");
        assert_eq!(first.drives()[0], PathBuf::from("/data/10.0.0.1:9000/d0"));
        assert_eq!(first.drives()[3], PathBuf::from("/data/10.0.0.1:9000/d3"));
        assert_eq!(second.node(), "10.0.0.2:9000");

        assert!(environment_from(layout("10.0.0.9:9000")).is_none(), "no local drive, no environment");
    }

    #[tokio::test]
    async fn engine_dirs_are_created_and_removed_on_every_local_drive() {
        let root = tempfile::tempdir().expect("tempdir");
        let drives: Vec<_> = (0..3).map(|i| root.path().join(format!("d{i}"))).collect();
        let environment = NodeEnvironment::new("a:9000".to_owned(), drives.clone(), PlacementView::default());
        let bucket = BucketContext {
            bucket: "kv".to_owned(),
            incarnation: 7,
            binding: BucketEngineBinding {
                engine: EngineName::new("kv-cache").expect("valid engine name"),
                config_version: 1,
                config: serde_json::Value::Null,
            },
        };

        create_engine_dirs(&environment, &bucket).await.expect("create");
        let dirs = environment.engine_dirs(&bucket.binding.engine, bucket.incarnation);
        assert!(dirs.iter().all(|dir| dir.is_dir()));
        tokio::fs::write(dirs[0].join("segment-0"), b"data").await.expect("write");

        remove_engine_dirs(&environment, &bucket).await.expect("remove");
        assert!(dirs.iter().all(|dir| !dir.exists()));
        remove_engine_dirs(&environment, &bucket)
            .await
            .expect("removing twice is fine");
    }
}
