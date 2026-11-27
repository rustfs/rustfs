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

//! Official DST campaign. Runs 4x4, then 3x2, in this process.

use rustfs_dst::{CampaignConfig, Topology, render_report, run_topologies};
use std::env;
use std::fs;
use std::process::ExitCode;
use std::time::Duration;

fn main() -> ExitCode {
    let mut config = CampaignConfig::default();
    let mut commit = None;
    let mut report_path = None;
    let mut checkpoint = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seeds" => {
                let Some(value) = args.next() else {
                    eprintln!("--seeds needs START-END or a comma list");
                    return ExitCode::from(2);
                };
                match parse_seeds(&value) {
                    Ok(seeds) => config.seeds = seeds,
                    Err(error) => {
                        eprintln!("{error}");
                        return ExitCode::from(2);
                    }
                }
            }
            "--steps" => config.steps = parse_next(&mut args, "--steps"),
            "--budget-secs" => {
                let secs = u64::from(parse_next(&mut args, "--budget-secs"));
                config.budget = Some(Duration::from_secs(secs));
            }
            "--max-payload" => config.max_payload = parse_next(&mut args, "--max-payload") as usize,
            "--max-objects" => config.max_objects = parse_next(&mut args, "--max-objects") as usize,
            "--commit" => commit = args.next(),
            "--report" => report_path = args.next(),
            "--checkpoint" => checkpoint = args.next(),
            "--no-minimize" => config.minimize = false,
            "--help" | "-h" => {
                print_help();
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument {other}");
                print_help();
                return ExitCode::from(2);
            }
        }
    }

    let topologies = match Topology::official() {
        Ok(topologies) => topologies,
        Err(error) => {
            eprintln!("layout: {error}");
            return ExitCode::from(1);
        }
    };
    let existing = checkpoint
        .as_ref()
        .and_then(|path| fs::read_to_string(path).ok())
        .unwrap_or_default();

    let mut report = rustfs_dst::CampaignReport {
        entries: Vec::new(),
        stopped_early: false,
        next_topology: None,
        next_seed: None,
    };
    for topology in &topologies {
        let mut topology_config = config.clone();
        topology_config
            .seeds
            .retain(|seed| !checkpoint_has(&existing, &topology.name, *seed));
        if topology_config.seeds.is_empty() {
            continue;
        }
        match run_topologies(std::slice::from_ref(topology), &topology_config) {
            Ok(partial) => {
                let stopped = partial.stopped_early;
                report.stopped_early = stopped;
                report.next_topology = partial.next_topology;
                report.next_seed = partial.next_seed;
                report.entries.extend(partial.entries);
                if stopped {
                    break;
                }
            }
            Err(error) => {
                eprintln!("campaign: {error}");
                return ExitCode::from(1);
            }
        }
    }
    let rendered = render_report(&report, commit.as_deref(), &topologies);
    print!("{rendered}");
    if let Some(path) = &report_path
        && let Err(error) = fs::write(path, &rendered)
    {
        eprintln!("report: {error}");
        return ExitCode::from(1);
    }
    if let Some(path) = &checkpoint {
        let mut lines = String::new();
        if let Some(commit) = &commit {
            lines.push_str(&format!("commit {commit}\n"));
        }
        for line in existing
            .lines()
            .filter(|line| line.starts_with("ok ") || line.starts_with("fail "))
        {
            lines.push_str(line);
            lines.push('\n');
        }
        for entry in &report.entries {
            if entry.passed {
                lines.push_str(&format!("ok {} {}\n", entry.topology, entry.seed));
            } else if let Some(failure) = &entry.failure {
                lines.push_str(&format!(
                    "fail {} {} step={} minimal={} invariant={}\n",
                    failure.topology, failure.seed, failure.step, failure.minimal_step, failure.invariant
                ));
            }
        }
        if let Err(error) = fs::write(path, lines) {
            eprintln!("checkpoint: {error}");
            return ExitCode::from(1);
        }
    }
    if report.entries.iter().any(|entry| !entry.passed) || report.stopped_early {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn parse_next(args: &mut impl Iterator<Item = String>, flag: &str) -> u32 {
    args.next().and_then(|value| value.parse().ok()).unwrap_or_else(|| {
        eprintln!("{flag} needs a number");
        std::process::exit(2);
    })
}

fn parse_seeds(value: &str) -> Result<Vec<u64>, String> {
    if let Some((start, end)) = value.split_once('-') {
        let start: u64 = start.parse().map_err(|_| format!("bad seed {start}"))?;
        let end: u64 = end.parse().map_err(|_| format!("bad seed {end}"))?;
        if end < start {
            return Err(format!("seed range {value} is inverted"));
        }
        return Ok((start..=end).collect());
    }
    value
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| part.parse::<u64>().map_err(|_| format!("bad seed {part}")))
        .collect()
}

fn checkpoint_has(text: &str, topology: &str, seed: u64) -> bool {
    text.lines().any(|line| {
        let mut parts = line.split_whitespace();
        let kind = parts.next();
        let recorded_topology = parts.next();
        let recorded = parts.next().and_then(|value| value.parse::<u64>().ok());
        matches!(kind, Some("ok" | "fail")) && recorded_topology == Some(topology) && recorded == Some(seed)
    })
}

fn print_help() {
    eprintln!(
        "dst-campaign [--seeds 1-24] [--steps 400] [--budget-secs 1800] \
         [--max-payload 4096] [--max-objects 8] [--commit SHA] \
         [--report path] [--checkpoint path] [--no-minimize]"
    );
}
