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

//! Sequential campaign runner.
//!
//! Official topologies run one after another in a single thread. A failing
//! seed is recorded and minimized; the campaign continues with the next seed.

use crate::topology::Topology;
use crate::world::{FaultRates, Violation, World, WorldConfig};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct CampaignConfig {
    pub seeds: Vec<u64>,
    pub steps: u32,
    pub rates: FaultRates,
    pub op_timeout_us: u64,
    pub max_objects: usize,
    pub max_payload: usize,
    pub minimize: bool,
    pub budget: Option<std::time::Duration>,
}

impl Default for CampaignConfig {
    fn default() -> Self {
        Self {
            seeds: (1..=24).collect(),
            steps: 400,
            rates: FaultRates::default(),
            op_timeout_us: 100_000,
            max_objects: 8,
            max_payload: 4096,
            minimize: true,
            budget: Some(std::time::Duration::from_secs(1800)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SeedFailure {
    pub topology: String,
    pub seed: u64,
    pub step: u32,
    pub minimal_step: u32,
    pub invariant: String,
    pub detail: String,
    pub trace: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct SeedReport {
    pub topology: String,
    pub seed: u64,
    pub steps: u32,
    pub passed: bool,
    pub elapsed_ms: u128,
    pub failure: Option<SeedFailure>,
}

#[derive(Clone, Debug)]
pub struct CampaignReport {
    pub entries: Vec<SeedReport>,
    pub stopped_early: bool,
    pub next_topology: Option<String>,
    pub next_seed: Option<u64>,
}

pub fn run_official(config: &CampaignConfig) -> Result<CampaignReport, String> {
    let topologies = Topology::official().map_err(|error| error.to_string())?;
    run_topologies(&topologies, config)
}

pub fn run_topologies(topologies: &[Topology], config: &CampaignConfig) -> Result<CampaignReport, String> {
    let started = Instant::now();
    let mut entries = Vec::new();
    for topology in topologies {
        for &seed in &config.seeds {
            if config.budget.is_some_and(|budget| started.elapsed() >= budget) {
                return Ok(CampaignReport {
                    entries,
                    stopped_early: true,
                    next_topology: Some(topology.name.clone()),
                    next_seed: Some(seed),
                });
            }
            let seed_started = Instant::now();
            match run_seed(topology, seed, config, false) {
                Ok(()) => entries.push(SeedReport {
                    topology: topology.name.clone(),
                    seed,
                    steps: config.steps,
                    passed: true,
                    elapsed_ms: seed_started.elapsed().as_millis(),
                    failure: None,
                }),
                Err(violation) => {
                    let (minimal_step, trace) = if config.minimize {
                        minimize(topology, seed, violation.step, config)
                    } else {
                        (violation.step, Vec::new())
                    };
                    entries.push(SeedReport {
                        topology: topology.name.clone(),
                        seed,
                        steps: config.steps,
                        passed: false,
                        elapsed_ms: seed_started.elapsed().as_millis(),
                        failure: Some(SeedFailure {
                            topology: topology.name.clone(),
                            seed,
                            step: violation.step,
                            minimal_step,
                            invariant: violation.invariant,
                            detail: violation.detail,
                            trace,
                        }),
                    });
                }
            }
        }
    }
    Ok(CampaignReport {
        entries,
        stopped_early: false,
        next_topology: None,
        next_seed: None,
    })
}

pub fn run_seed(topology: &Topology, seed: u64, config: &CampaignConfig, trace: bool) -> Result<(), Violation> {
    let mut world = World::new(
        topology.clone(),
        seed,
        WorldConfig {
            rates: config.rates.clone(),
            op_timeout_us: config.op_timeout_us,
            max_objects: config.max_objects,
            max_payload: config.max_payload,
            trace,
        },
    )
    .map_err(|detail| Violation {
        step: 0,
        invariant: "setup".to_string(),
        detail,
    })?;
    for _ in 0..config.steps {
        world.step()?;
    }
    world.check()?;
    Ok(())
}

fn minimize(topology: &Topology, seed: u64, failed_step: u32, config: &CampaignConfig) -> (u32, Vec<String>) {
    let mut low = 1_u32;
    let mut high = failed_step.max(1);
    while low < high {
        let mid = low + (high - low) / 2;
        let mut probe = config.clone();
        probe.steps = mid;
        probe.minimize = false;
        if run_seed(topology, seed, &probe, false).is_err() {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    let mut traced = config.clone();
    traced.steps = low;
    traced.minimize = false;
    let Ok(mut world) = World::new(
        topology.clone(),
        seed,
        WorldConfig {
            rates: config.rates.clone(),
            op_timeout_us: config.op_timeout_us,
            max_objects: config.max_objects,
            max_payload: config.max_payload,
            trace: true,
        },
    ) else {
        return (low, Vec::new());
    };
    let mut failed = None;
    for _ in 0..low {
        if let Err(violation) = world.step() {
            failed = Some(violation);
            break;
        }
    }
    let trace = world.trace;
    let step = failed.map(|violation| violation.step).unwrap_or(low);
    (step, trace)
}

pub fn render_report(report: &CampaignReport, commit: Option<&str>, topologies: &[Topology]) -> String {
    let mut out = String::new();
    out.push_str("# RustFS 1.0.7 DST campaign\n\n");
    if let Some(commit) = commit {
        out.push_str(&format!("Source commit: `{commit}`\n\n"));
    }
    out.push_str("Topologies ran sequentially in one process, in the order below.\n\n");
    for topology in topologies {
        let set = topology.sets.first();
        out.push_str(&format!("- `{}` volume `{}`\n", topology.name, topology.volume));
        if let Some(set) = set {
            out.push_str(&format!(
                "  nodes={} sets={} drives={} data={} parity={} read_quorum={} write_quorum={} exclusive_lock={} shared_lock={}\n",
                topology.nodes.len(),
                topology.sets.len(),
                set.drives.len(),
                set.data_blocks,
                set.parity_blocks,
                set.read_quorum,
                set.write_quorum,
                set.exclusive_lock_quorum,
                set.shared_lock_quorum
            ));
        }
    }
    out.push('\n');
    let passed = report.entries.iter().filter(|entry| entry.passed).count();
    let failed = report.entries.len().saturating_sub(passed);
    out.push_str(&format!("Passed {passed}, failed {failed}.\n"));
    if report.stopped_early {
        out.push_str(&format!(
            "Stopped early. Resume at topology {:?} seed {:?}.\n",
            report.next_topology, report.next_seed
        ));
    }
    out.push('\n');
    for entry in &report.entries {
        if entry.passed {
            out.push_str(&format!(
                "- {} seed {} passed steps {} in {} ms\n",
                entry.topology, entry.seed, entry.steps, entry.elapsed_ms
            ));
        } else if let Some(failure) = &entry.failure {
            out.push_str(&format!(
                "- {} seed {} FAILED step {} minimal {} invariant `{}`: {}\n",
                failure.topology, failure.seed, failure.step, failure.minimal_step, failure.invariant, failure.detail
            ));
        }
    }
    out
}
