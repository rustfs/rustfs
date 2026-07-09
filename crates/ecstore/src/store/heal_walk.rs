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

//! Store-level entry point for the per-erasure-set disk-walk UNION heal
//! enumerator (backlog#920). Bounds-checks `(pool_idx, set_idx)` (mirroring
//! `handle_get_disks`) and delegates to the target `SetDisks`.

use super::*;
pub use crate::set_disk::HealWalkVersion;

impl ECStore {
    /// Walk a specific erasure set's disks and return one page of the cross-disk
    /// UNION of versions (see `SetDisks::heal_walk_versions_page`). `pool_idx` and
    /// `set_idx` are bounds-checked against the live pool topology.
    #[instrument(level = "debug", skip(self))]
    #[allow(clippy::too_many_arguments)]
    pub async fn heal_walk_versions_page(
        &self,
        pool_idx: usize,
        set_idx: usize,
        bucket: &str,
        prefix: &str,
        forward_to: Option<&str>,
        batch_objects: usize,
        version_budget: usize,
    ) -> Result<(Vec<HealWalkVersion>, Option<String>, bool)> {
        if pool_idx >= self.pools.len() || set_idx >= self.pools[pool_idx].disk_set.len() {
            return Err(Error::other(format!(
                "heal disk-walk: invalid erasure set (pool index {pool_idx}, set index {set_idx}, pool count {})",
                self.pools.len()
            )));
        }

        self.pools[pool_idx].disk_set[set_idx]
            .heal_walk_versions_page(bucket, prefix, forward_to, batch_objects, version_budget)
            .await
            .map_err(Error::from)
    }
}
