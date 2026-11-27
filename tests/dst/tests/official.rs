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

use rustfs_dst::{CampaignConfig, FaultRates, Topology, World, WorldConfig, run_topologies};
use std::time::Duration;

fn quiet(topology_name: &str) -> (Topology, World) {
    let topology = Topology::official()
        .expect("layouts")
        .into_iter()
        .find(|topology| topology.name == topology_name)
        .expect(topology_name);
    let world = World::new(
        topology.clone(),
        7,
        WorldConfig {
            rates: FaultRates::none(),
            ..WorldConfig::default()
        },
    )
    .expect("world");
    (topology, world)
}

#[test]
fn both_topologies_roundtrip_without_faults() {
    for name in ["4x4", "3x2"] {
        let (_topology, mut world) = quiet(name);
        let payload = b"read-after-write".to_vec();
        world.put_quiet("obj-0", &payload).expect("put");
        world
            .check()
            .unwrap_or_else(|violation| panic!("{name}: {}", violation.detail));
    }
}

#[test]
fn node_crash_keeps_bytes_when_media_remains() {
    for name in ["4x4", "3x2"] {
        let (_topology, mut world) = quiet(name);
        world.put_quiet("obj-0", b"durable").expect("put");
        world.crash_every_node();
        world
            .check()
            .unwrap_or_else(|violation| panic!("{name}: {}", violation.detail));
    }
}

#[test]
fn short_campaign_runs_4x4_then_3x2() {
    let config = CampaignConfig {
        seeds: vec![1, 2],
        steps: 20,
        rates: FaultRates::default(),
        minimize: false,
        budget: Some(Duration::from_secs(120)),
        ..CampaignConfig::default()
    };
    let report = run_topologies(&Topology::official().expect("layouts"), &config).expect("campaign");
    let order: Vec<_> = report.entries.iter().map(|entry| entry.topology.as_str()).collect();
    assert_eq!(order, ["4x4", "4x4", "3x2", "3x2"]);
    assert!(!report.stopped_early);
    for entry in &report.entries {
        if let Some(failure) = &entry.failure {
            panic!(
                "{} seed {} step {} {}: {}",
                failure.topology, failure.seed, failure.step, failure.invariant, failure.detail
            );
        }
    }
}
