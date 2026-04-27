#![allow(unused_imports)]
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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use rustfs_common::data_usage::TierStats;
use sha2::Sha256;
use std::collections::HashMap;
use std::ops::Sub;
use time::OffsetDateTime;
use tracing::{error, warn};

pub type DailyAllTierStats = HashMap<String, LastDayTierStats>;

#[derive(Clone)]
pub struct LastDayTierStats {
    bins: [TierStats; 24],
    updated_at: OffsetDateTime,
}

impl Default for LastDayTierStats {
    fn default() -> Self {
        Self {
            bins: Default::default(),
            updated_at: OffsetDateTime::now_utc(),
        }
    }
}

impl LastDayTierStats {
    pub fn add_stats(&mut self, ts: TierStats) {
        let mut now = OffsetDateTime::now_utc();
        self.forward_to(&mut now);

        let now_idx = now.hour() as usize;
        self.bins[now_idx] = self.bins[now_idx].add(&ts);
    }

    pub fn total(&self) -> TierStats {
        self.bins.iter().fold(TierStats::default(), |acc, bin| acc.add(bin))
    }

    fn forward_to(&mut self, t: &mut OffsetDateTime) {
        if t.unix_timestamp() == 0 {
            *t = OffsetDateTime::now_utc();
        }

        let since = t.sub(self.updated_at).whole_hours();
        if since < 1 {
            return;
        }

        let (idx, mut last_idx) = (t.hour(), self.updated_at.hour());

        self.updated_at = *t;

        if since >= 24 {
            self.bins = [TierStats::default(); 24];
            return;
        }

        while last_idx != idx {
            last_idx = (last_idx + 1) % 24;
            self.bins[last_idx as usize] = TierStats::default();
        }
    }

    #[allow(dead_code)]
    fn merge(&self, m: LastDayTierStats) -> LastDayTierStats {
        let mut cl = self.clone();
        let mut cm = m;
        let mut merged = LastDayTierStats::default();

        if cl.updated_at.unix_timestamp() > cm.updated_at.unix_timestamp() {
            cm.forward_to(&mut cl.updated_at);
            merged.updated_at = cl.updated_at;
        } else {
            cl.forward_to(&mut cm.updated_at);
            merged.updated_at = cm.updated_at;
        }

        for (i, _) in cl.bins.iter().enumerate() {
            merged.bins[i] = cl.bins[i].add(&cm.bins[i]);
        }

        merged
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn total_sums_all_recorded_stats() {
        let mut stats = LastDayTierStats::default();
        stats.add_stats(TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        });
        stats.add_stats(TierStats {
            total_size: 20,
            num_versions: 2,
            num_objects: 0,
        });

        assert_eq!(
            stats.total(),
            TierStats {
                total_size: 30,
                num_versions: 3,
                num_objects: 1,
            }
        );
    }
}
