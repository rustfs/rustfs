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
use std::sync::{Arc, RwLock};

use crate::name::EngineName;
use crate::placement::PlacementView;

/// System directory on every drive, shared with the object layer's metadata.
pub const SYSTEM_DIR: &str = ".rustfs.sys";
/// Directory under [`SYSTEM_DIR`] holding engine data.
pub const ENGINES_DIR: &str = "engines";

/// Where an engine keeps one bucket incarnation's data on a drive:
/// `<drive>/.rustfs.sys/engines/<engine>/<incarnation as 32 hex digits>`.
/// Keyed by incarnation so a recreated bucket never sees the old data.
pub fn engine_dir(drive_root: &Path, engine: &EngineName, incarnation: u128) -> PathBuf {
    drive_root
        .join(SYSTEM_DIR)
        .join(ENGINES_DIR)
        .join(engine.as_str())
        .join(format!("{incarnation:032x}"))
}

/// This node as engines see it: its identity in the placement view, its
/// local drives in placement drive order, and the current placement view.
#[derive(Debug)]
pub struct NodeEnvironment {
    node: String,
    drives: Vec<PathBuf>,
    placement: RwLock<Arc<PlacementView>>,
}

impl NodeEnvironment {
    pub fn new(node: String, drives: Vec<PathBuf>, placement: PlacementView) -> Self {
        Self {
            node,
            drives,
            placement: RwLock::new(Arc::new(placement)),
        }
    }

    /// The node's identity in the placement view (`host:port`).
    pub fn node(&self) -> &str {
        &self.node
    }

    /// Root of each local drive; the index is the placement drive index.
    pub fn drives(&self) -> &[PathBuf] {
        &self.drives
    }

    pub fn placement(&self) -> Arc<PlacementView> {
        Arc::clone(&self.placement.read().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    /// Replaces the placement view; returns whether its version changed.
    pub fn set_placement(&self, placement: PlacementView) -> bool {
        let mut current = self.placement.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        let changed = current.version != placement.version;
        *current = Arc::new(placement);
        changed
    }

    /// The bucket incarnation's engine directory on every local drive.
    pub fn engine_dirs(&self, engine: &EngineName, incarnation: u128) -> Vec<PathBuf> {
        self.drives
            .iter()
            .map(|drive| engine_dir(drive, engine, incarnation))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::NodeTargets;

    #[test]
    fn engine_dirs_are_keyed_by_engine_and_incarnation() {
        let engine = EngineName::new("kv-cache").expect("valid engine name");
        assert_eq!(
            engine_dir(Path::new("/data/d1"), &engine, 0xab),
            PathBuf::from("/data/d1/.rustfs.sys/engines/kv-cache/000000000000000000000000000000ab")
        );

        let environment = NodeEnvironment::new(
            "a:9000".to_owned(),
            vec![PathBuf::from("/d1"), PathBuf::from("/d2")],
            PlacementView::default(),
        );
        let dirs = environment.engine_dirs(&engine, 7);
        assert_eq!(dirs.len(), 2);
        assert!(dirs[1].starts_with("/d2/.rustfs.sys/engines/kv-cache"));
    }

    #[test]
    fn placement_updates_report_version_changes() {
        let environment = NodeEnvironment::new("a:9000".to_owned(), Vec::new(), PlacementView::default());
        let view = PlacementView::from_nodes(vec![NodeTargets {
            node: "a:9000".to_owned(),
            drives: 4,
            online: true,
        }]);
        assert!(environment.set_placement(view.clone()));
        assert!(!environment.set_placement(view.clone()));
        assert_eq!(*environment.placement(), view);
    }
}
