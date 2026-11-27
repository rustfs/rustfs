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

//! Official cluster shapes, expanded by RustFS 1.0.7's layout selector.
//!
//! Both campaigns use one pool and `set_drive_count = 0` (the default when
//! `RUSTFS_ERASURE_SET_DRIVE_COUNT` is unset). Parity is `default_parity_count`
//! for that set width. Lock clients are the distinct node hosts in the set.

use rustfs_ecstore::dst_seam::{
    default_parity_count, exclusive_lock_quorum, layout_sets, read_quorum, shared_lock_quorum, write_quorum,
};
use std::collections::BTreeSet;
use std::io;

/// Stable SipHash key for set selection. One-set layouts ignore it.
pub const FORMAT_ID: [u8; 16] = *b"rustfs-dst-1.0.7";

pub const DST_BUCKET: &str = "dst-bucket";

/// 4 nodes x 4 disks, then 3 nodes x 2 disks. Campaigns run in this order.
pub const OFFICIAL_VOLUMES: [(&str, &str); 2] = [
    ("4x4", "http://node{1...4}:9000/data{1...4}"),
    ("3x2", "http://node{1...3}:9000/data{1...2}"),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topology {
    pub name: String,
    pub volume: String,
    pub nodes: Vec<String>,
    pub sets: Vec<SetPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetPlan {
    pub drives: Vec<DrivePlan>,
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub read_quorum: usize,
    pub write_quorum: usize,
    pub exclusive_lock_quorum: usize,
    pub shared_lock_quorum: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrivePlan {
    pub node_index: usize,
    pub endpoint: String,
}

impl Topology {
    pub fn load(name: &str, volume: &str) -> io::Result<Self> {
        let sets = layout_sets(volume)?;
        if sets.is_empty() {
            return Err(io::Error::other(format!("{volume} produced no erasure sets")));
        }
        let mut hosts = BTreeSet::new();
        for set in &sets {
            for endpoint in set {
                let host = host_of(endpoint).ok_or_else(|| io::Error::other(format!("cannot parse host in {endpoint}")))?;
                hosts.insert(host.to_string());
            }
        }
        let nodes: Vec<String> = hosts.into_iter().collect();
        let mut plans = Vec::with_capacity(sets.len());
        for set in sets {
            let mut drives = Vec::with_capacity(set.len());
            for endpoint in set {
                let host = host_of(&endpoint).ok_or_else(|| io::Error::other(format!("cannot parse host in {endpoint}")))?;
                let node_index = nodes
                    .iter()
                    .position(|name| name == host)
                    .ok_or_else(|| io::Error::other(format!("host {host} missing from node list")))?;
                drives.push(DrivePlan { node_index, endpoint });
            }
            let drive_count = drives.len();
            let parity = default_parity_count(drive_count);
            if parity > drive_count {
                return Err(io::Error::other(format!("parity {parity} exceeds drives {drive_count}")));
            }
            let lock_clients = drives.iter().map(|drive| drive.node_index).collect::<BTreeSet<_>>().len();
            plans.push(SetPlan {
                drives,
                data_blocks: drive_count - parity,
                parity_blocks: parity,
                read_quorum: read_quorum(drive_count, parity),
                write_quorum: write_quorum(drive_count, parity),
                exclusive_lock_quorum: exclusive_lock_quorum(lock_clients),
                shared_lock_quorum: shared_lock_quorum(lock_clients),
            });
        }
        Ok(Self {
            name: name.to_string(),
            volume: volume.to_string(),
            nodes,
            sets: plans,
        })
    }

    pub fn official() -> io::Result<Vec<Self>> {
        OFFICIAL_VOLUMES
            .iter()
            .map(|(name, volume)| Self::load(name, volume))
            .collect()
    }
}

pub fn host_of(endpoint: &str) -> Option<&str> {
    let rest = endpoint.split("://").nth(1)?;
    let host = rest.split('/').next()?;
    if host.is_empty() { None } else { Some(host) }
}

#[cfg(test)]
mod tests {
    use super::Topology;

    #[test]
    fn official_layouts_follow_default_set_selection() {
        let topologies = Topology::official().expect("official volumes should parse");
        assert_eq!(topologies.len(), 2);

        let four = &topologies[0];
        assert_eq!(four.name, "4x4");
        assert_eq!(four.nodes.len(), 4);
        assert_eq!(four.sets.len(), 1);
        assert_eq!(four.sets[0].drives.len(), 16);
        assert_eq!(four.sets[0].parity_blocks, 4);
        assert_eq!(four.sets[0].read_quorum, 12);
        assert_eq!(four.sets[0].write_quorum, 12);
        assert_eq!(four.sets[0].exclusive_lock_quorum, 3);
        assert_eq!(four.sets[0].shared_lock_quorum, 2);
        assert!(four.sets[0].write_quorum >= four.sets[0].read_quorum);
        assert!(four.sets[0].drives.len() - four.sets[0].write_quorum < four.sets[0].read_quorum);
        for node in 0..4 {
            assert_eq!(four.sets[0].drives.iter().filter(|drive| drive.node_index == node).count(), 4);
        }

        let three = &topologies[1];
        assert_eq!(three.name, "3x2");
        assert_eq!(three.nodes.len(), 3);
        assert_eq!(three.sets.len(), 1);
        assert_eq!(three.sets[0].drives.len(), 6);
        assert_eq!(three.sets[0].parity_blocks, 3);
        assert_eq!(three.sets[0].data_blocks, 3);
        assert_eq!(three.sets[0].read_quorum, 3);
        assert_eq!(three.sets[0].write_quorum, 4);
        assert_eq!(three.sets[0].exclusive_lock_quorum, 2);
        assert_eq!(three.sets[0].shared_lock_quorum, 2);
        assert!(three.sets[0].write_quorum >= three.sets[0].read_quorum);
        assert!(three.sets[0].drives.len() - three.sets[0].write_quorum < three.sets[0].read_quorum);
        for node in 0..3 {
            assert_eq!(three.sets[0].drives.iter().filter(|drive| drive.node_index == node).count(), 2);
        }
    }
}
