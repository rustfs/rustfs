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

use std::slice::Iter;

#[derive(Debug, Default, Clone)]
pub struct PoolAvailableSpace {
    pub index: usize,
    pub available: u64,    // in bytes
    pub max_used_pct: u64, // Used disk percentage of most filled disk, rounded down.
}

#[derive(Debug, Default, Clone)]
pub struct ServerPoolsAvailableSpace(pub(crate) Vec<PoolAvailableSpace>);

impl ServerPoolsAvailableSpace {
    pub fn iter(&self) -> Iter<'_, PoolAvailableSpace> {
        self.0.iter()
    }

    // TotalAvailable - total available space
    pub fn total_available(&self) -> u64 {
        let mut total = 0;
        for pool in &self.0 {
            total += pool.available;
        }
        total
    }

    // FilterMaxUsed will filter out any pools that has used percent bigger than max,
    // unless all have that, in which case all are preserved.
    pub fn filter_max_used(&mut self, max: u64) {
        if self.0.len() <= 1 {
            // Nothing to do.
            return;
        }
        let mut ok = false;
        for pool in &self.0 {
            if pool.available > 0 && pool.max_used_pct < max {
                ok = true;
                break;
            }
        }
        if !ok {
            // All above limit.
            // Do not modify
            return;
        }

        // Remove entries that are above.
        for pool in self.0.iter_mut() {
            if pool.available > 0 && pool.max_used_pct < max {
                continue;
            }
            pool.available = 0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_available_space_creation_preserves_fields() {
        let space = PoolAvailableSpace {
            index: 0,
            available: 1000,
            max_used_pct: 50,
        };

        assert_eq!(space.index, 0);
        assert_eq!(space.available, 1000);
        assert_eq!(space.max_used_pct, 50);
    }

    #[test]
    fn server_pools_available_space_filter_keeps_shape_and_updates_availability() {
        let mut spaces = ServerPoolsAvailableSpace(vec![
            PoolAvailableSpace {
                index: 0,
                available: 1000,
                max_used_pct: 50,
            },
            PoolAvailableSpace {
                index: 1,
                available: 2000,
                max_used_pct: 80,
            },
        ]);

        assert_eq!(spaces.total_available(), 3000);

        spaces.filter_max_used(60);

        assert_eq!(spaces.0.len(), 2);
        assert_eq!(spaces.0[0].index, 0);
        assert_eq!(spaces.0[0].available, 1000);
        assert_eq!(spaces.0[1].available, 0);
        assert_eq!(spaces.total_available(), 1000);
    }

    #[test]
    fn server_pools_available_space_iter_preserves_pool_order() {
        let spaces = ServerPoolsAvailableSpace(vec![PoolAvailableSpace {
            index: 0,
            available: 1000,
            max_used_pct: 50,
        }]);

        let indexes = spaces.iter().map(|space| space.index).collect::<Vec<_>>();

        assert_eq!(indexes, vec![0]);
    }
}
