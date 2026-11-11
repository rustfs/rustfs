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

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

/// A drive of a node that an engine can place data on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TargetId {
    /// The node's stable identity: its endpoint `host:port` as configured.
    pub node: String,
    /// Index of the drive among the node's engine drives.
    pub drive: u16,
}

impl TargetId {
    fn seed(&self) -> u64 {
        xxh3_64_with_seed(self.node.as_bytes(), u64::from(self.drive) ^ xxh3_64(b"rustfs.bucket-engine.target"))
    }

    /// Rendezvous score of `key` on this target; the highest score owns the key.
    pub fn score(&self, key: &[u8]) -> u64 {
        xxh3_64_with_seed(key, self.seed())
    }
}

/// One node and the engine drives it contributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTargets {
    pub node: String,
    pub drives: u16,
    pub online: bool,
}

/// The cluster as engines see it: nodes in configured endpoint order, their
/// drives, and liveness. The version is derived from membership (nodes and
/// drive counts) so every node computes the same version from the same
/// topology and requests can carry it for owners to reject stale ones.
/// Liveness is each node's own observation and does not change the version.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlacementView {
    pub version: u64,
    pub nodes: Vec<NodeTargets>,
}

impl PlacementView {
    /// Builds a view and derives its version from membership.
    pub fn from_nodes(nodes: Vec<NodeTargets>) -> Self {
        let mut membership = Vec::new();
        for node in &nodes {
            membership.extend_from_slice(&(node.node.len() as u64).to_be_bytes());
            membership.extend_from_slice(node.node.as_bytes());
            membership.extend_from_slice(&node.drives.to_be_bytes());
        }
        Self {
            version: xxh3_64(&membership),
            nodes,
        }
    }

    /// Every target in the view, online or not, in node then drive order.
    pub fn targets(&self) -> impl Iterator<Item = TargetId> + '_ {
        self.nodes.iter().flat_map(|node| {
            (0..node.drives).map(move |drive| TargetId {
                node: node.node.clone(),
                drive,
            })
        })
    }

    pub fn is_online(&self, node: &str) -> bool {
        self.nodes.iter().any(|candidate| candidate.node == node && candidate.online)
    }

    /// The targets that own `key`: the highest-scoring target, then the
    /// highest-scoring targets on other nodes, up to `replicas` targets. Offline
    /// targets keep their rank, so ownership does not move while a node is down;
    /// callers treat an offline owner as a miss. Returns fewer targets when the
    /// view has fewer distinct nodes than `replicas`.
    pub fn owners(&self, key: &[u8], replicas: usize) -> Vec<TargetId> {
        let mut ranked: Vec<(u64, TargetId)> = self.targets().map(|target| (target.score(key), target)).collect();
        // Ties are broken by target order so every caller ranks identically.
        ranked.sort_by(|(left_score, left), (right_score, right)| right_score.cmp(left_score).then_with(|| left.cmp(right)));
        let mut owners: Vec<TargetId> = Vec::with_capacity(replicas);
        for (_, target) in ranked {
            if owners.len() == replicas {
                break;
            }
            if owners.iter().all(|owner| owner.node != target.node) {
                owners.push(target);
            }
        }
        owners
    }

    /// The single owner of `key`, if the view has any target.
    pub fn owner(&self, key: &[u8]) -> Option<TargetId> {
        self.owners(key, 1).into_iter().next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn version_tracks_membership_but_not_liveness() {
        let node = |name: &str, drives: u16, online: bool| NodeTargets {
            node: name.to_owned(),
            drives,
            online,
        };
        let base = PlacementView::from_nodes(vec![node("a:9000", 4, true), node("b:9000", 4, true)]);
        let same = PlacementView::from_nodes(vec![node("a:9000", 4, true), node("b:9000", 4, false)]);
        assert_eq!(base.version, same.version, "liveness is local and must not change the version");

        for changed in [
            vec![node("a:9000", 4, true), node("b:9000", 3, true)],
            vec![node("a:9000", 4, true)],
            vec![node("a:9000", 4, true), node("c:9000", 4, true)],
            vec![node("b:9000", 4, true), node("a:9000", 4, true)],
        ] {
            assert_ne!(base.version, PlacementView::from_nodes(changed).version);
        }
    }

    fn view(nodes: &[(&str, u16)]) -> PlacementView {
        PlacementView {
            version: 1,
            nodes: nodes
                .iter()
                .map(|(node, drives)| NodeTargets {
                    node: (*node).to_owned(),
                    drives: *drives,
                    online: true,
                })
                .collect(),
        }
    }

    fn keys(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(|i| format!("hicache/kimi-k3/{i:016x}").into_bytes()).collect()
    }

    #[test]
    fn owners_are_deterministic_across_view_copies() {
        let a = view(&[("10.0.0.1:9000", 4), ("10.0.0.2:9000", 4)]);
        let b = a.clone();
        for key in keys(1000) {
            assert_eq!(a.owners(&key, 2), b.owners(&key, 2));
        }
    }

    #[test]
    fn keys_spread_evenly_over_targets() {
        let v = view(&[("n1:9000", 4), ("n2:9000", 4), ("n3:9000", 4), ("n4:9000", 4)]);
        let mut counts: HashMap<TargetId, usize> = HashMap::new();
        let total = 64_000;
        for key in keys(total) {
            *counts.entry(v.owner(&key).expect("view has targets")).or_default() += 1;
        }
        assert_eq!(counts.len(), 16, "every target must own some keys");
        let expected = total / 16;
        for (target, count) in counts {
            assert!(
                count > expected * 85 / 100 && count < expected * 115 / 100,
                "{target:?} owns {count}, expected about {expected}"
            );
        }
    }

    #[test]
    fn adding_a_target_only_moves_keys_onto_it() {
        let before = view(&[("n1:9000", 4), ("n2:9000", 4)]);
        let after = view(&[("n1:9000", 4), ("n2:9000", 4), ("n3:9000", 1)]);
        let new_target = TargetId {
            node: "n3:9000".to_owned(),
            drive: 0,
        };
        let mut moved = 0;
        for key in keys(9000) {
            let (old, new) = (before.owner(&key), after.owner(&key));
            if old != new {
                assert_eq!(new.as_ref(), Some(&new_target), "a key may only move to the added target");
                moved += 1;
            }
        }
        assert!(
            moved > 9000 / 9 * 80 / 100 && moved < 9000 / 9 * 120 / 100,
            "moved {moved} keys, expected about 1/9"
        );
    }

    #[test]
    fn replicas_land_on_distinct_nodes() {
        let v = view(&[("n1:9000", 4), ("n2:9000", 4), ("n3:9000", 4)]);
        for key in keys(1000) {
            let owners = v.owners(&key, 2);
            assert_eq!(owners.len(), 2);
            assert_ne!(owners[0].node, owners[1].node);
            assert_eq!(owners[0], v.owner(&key).expect("view has targets"), "the first replica is the owner");
        }
        assert_eq!(view(&[("n1:9000", 4)]).owners(b"k", 2).len(), 1, "one node yields one replica");
        assert!(PlacementView::default().owner(b"k").is_none());
    }

    #[test]
    fn offline_nodes_keep_their_keys() {
        let mut v = view(&[("n1:9000", 2), ("n2:9000", 2)]);
        let owners_before: Vec<_> = keys(500).iter().map(|key| v.owner(key)).collect();
        v.nodes[1].online = false;
        v.version += 1;
        let owners_after: Vec<_> = keys(500).iter().map(|key| v.owner(key)).collect();
        assert_eq!(owners_before, owners_after);
        assert!(!v.is_online("n2:9000"));
    }

    /// Clients route keys with their own copy of this hash (the SGLang HiCache
    /// backend pins the same vector); a change here breaks their routing.
    #[test]
    fn owners_match_the_pinned_client_vector() {
        let v = view(&[("10.0.0.1:9000", 4), ("10.0.0.2:9000", 4), ("10.0.0.3:9000", 2)]);
        for (key, node, drive) in [
            ("hicache/a", "10.0.0.1:9000", 2),
            ("hicache/b", "10.0.0.2:9000", 3),
            ("hicache/c", "10.0.0.1:9000", 2),
            ("hicache/d", "10.0.0.3:9000", 1),
            ("hicache/e", "10.0.0.2:9000", 3),
            ("hicache/f", "10.0.0.1:9000", 2),
        ] {
            let owner = v.owner(key.as_bytes()).expect("owner");
            assert_eq!((owner.node.as_str(), owner.drive), (node, drive), "{key}");
        }
    }
}
