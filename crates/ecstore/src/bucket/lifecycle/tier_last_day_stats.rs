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

use rustfs_data_usage::TierStats;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::ops::Sub;
use time::OffsetDateTime;
use tracing::{error, warn};

pub type DailyAllTierStats = HashMap<String, LastDayTierStats>;

/// One bin per hour of the rolling day. The bin index is the UTC hour, so the
/// array is a ring the writer ages forward rather than a queue.
pub const TIER_DAILY_STATS_BINS: usize = 24;

/// Interchange form of [`LastDayTierStats`] for the internode tier-stats RPC.
///
/// The in-memory type keeps its bins private because the ring is only
/// meaningful together with `updated_at`; this type carries both across the
/// wire and is validated back into the ring by [`LastDayTierStats::from_wire`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierDailyStatsWire {
    pub bins: Vec<TierStats>,
    /// Seconds since the Unix epoch. Bins are hour-resolution, so a coarser
    /// timestamp than the in-memory `OffsetDateTime` loses nothing.
    pub updated_at_unix_secs: i64,
}

#[derive(Clone, Debug)]
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

    /// The rolling ring as observed, without aging it forward.
    ///
    /// Only meaningful together with [`LastDayTierStats::updated_at`]: a bin
    /// belongs to the hour of its index within the day that ends at
    /// `updated_at`.
    pub fn bins(&self) -> &[TierStats; TIER_DAILY_STATS_BINS] {
        &self.bins
    }

    pub fn updated_at(&self) -> OffsetDateTime {
        self.updated_at
    }

    pub fn to_wire(&self) -> TierDailyStatsWire {
        TierDailyStatsWire {
            bins: self.bins.to_vec(),
            updated_at_unix_secs: self.updated_at.unix_timestamp(),
        }
    }

    /// Rebuild the ring from a peer's response.
    ///
    /// A ring of the wrong width or an unrepresentable timestamp is corrupt
    /// peer input, not a zero sample: it returns an error so the caller can
    /// report the node as non-reporting instead of merging a plausible but
    /// wrong day into a cluster total.
    pub fn from_wire(wire: TierDailyStatsWire) -> Result<Self, std::io::Error> {
        let bins: [TierStats; TIER_DAILY_STATS_BINS] = wire.bins.try_into().map_err(|bins: Vec<TierStats>| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("tier daily stats must carry {TIER_DAILY_STATS_BINS} bins, got {}", bins.len()),
            )
        })?;
        let updated_at = OffsetDateTime::from_unix_timestamp(wire.updated_at_unix_secs).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("tier daily stats carry an unrepresentable timestamp: {err}"),
            )
        })?;

        Ok(Self { bins, updated_at })
    }

    /// Combine two independently observed rings.
    ///
    /// Each node counts only the transitions it completed itself, so summing
    /// bins across nodes is a cluster total rather than a double count. The
    /// older ring is aged forward to the newer one's clock first, so a node
    /// that stopped transitioning hours ago contributes its still-current
    /// bins and not its expired ones.
    pub fn merge(&self, m: LastDayTierStats) -> LastDayTierStats {
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
    use time::Duration;

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

    fn sample(total_size: u64) -> TierStats {
        TierStats {
            total_size,
            num_versions: 1,
            num_objects: 1,
        }
    }

    #[test]
    fn wire_round_trip_preserves_the_ring_and_its_clock() {
        let mut stats = LastDayTierStats::default();
        stats.add_stats(sample(10));

        let restored = LastDayTierStats::from_wire(stats.to_wire()).expect("a ring this node produced must decode");

        assert_eq!(restored.bins(), stats.bins(), "every bin must survive the wire");
        assert_eq!(
            restored.updated_at().unix_timestamp(),
            stats.updated_at().unix_timestamp(),
            "the ring's clock must survive the wire"
        );
    }

    #[test]
    fn a_ring_of_the_wrong_width_is_rejected() {
        let mut wire = LastDayTierStats::default().to_wire();
        wire.bins.pop();

        let err = LastDayTierStats::from_wire(wire).expect_err("a short ring must not decode as a zero day");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn an_unrepresentable_clock_is_rejected() {
        let mut wire = LastDayTierStats::default().to_wire();
        wire.updated_at_unix_secs = i64::MIN;

        let err = LastDayTierStats::from_wire(wire).expect_err("an unrepresentable clock must not decode");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn merge_sums_two_nodes_that_transitioned_in_the_same_hour() {
        let mut left = LastDayTierStats::default();
        left.add_stats(sample(10));
        let mut right = LastDayTierStats::default();
        right.add_stats(sample(20));

        assert_eq!(
            left.merge(right).total(),
            TierStats {
                total_size: 30,
                num_versions: 2,
                num_objects: 2,
            },
            "each node counts only its own completions, so a merge is a cluster total"
        );
    }

    #[test]
    fn merge_ages_out_a_peer_ring_older_than_a_day() {
        let mut stale = LastDayTierStats::default();
        stale.add_stats(sample(10));
        stale.updated_at -= Duration::days(2);

        let mut fresh = LastDayTierStats::default();
        fresh.add_stats(sample(20));

        assert_eq!(
            fresh.merge(stale).total(),
            TierStats {
                total_size: 20,
                num_versions: 1,
                num_objects: 1,
            },
            "a node that stopped transitioning more than a day ago must not keep contributing"
        );
    }
}
