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

//! Channel 2: container/collector prefix stripping.
//!
//! RustFS defaults to stdout-only logging, so customer logs frequently come
//! from `kubectl logs` / `docker compose logs` / journald and each line is
//! wrapped in a collector prefix that must be removed before JSON parsing.
//!
//! Known, deliberate limitations (Phase 2 if real demand shows up):
//! - CRI `P` (partial) continuation lines are not re-joined across lines;
//!   each stripped fragment is processed on its own.
//! - Raw docker `json-file` driver files (`{"log":"...","stream":...}`)
//!   are not unwrapped; `docker logs` output is what customers actually send.

use regex::Regex;
use std::sync::LazyLock;

/// K8s CRI (containerd / CRI-O): `2026-07-15T06:23:01.123456789Z stdout F <payload>`.
static CRI: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}T\S+ (?:stdout|stderr) [FP] (.*)$").expect("static regex"));

/// docker compose: `rustfs-node1-1  | <payload>`.
static COMPOSE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\s+\| ?(.*)$").expect("static regex"));

/// journald/syslog: `Jul 15 06:23:01 host1 rustfs[1234]: <payload>`.
static JOURNALD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Z][a-z]{2} +\d{1,2} \d{2}:\d{2}:\d{2} \S+ [^:\s\[]+\[\d+\]: (.*)$").expect("static regex"));

/// Strips the first matching collector prefix; `None` when no prefix matches.
pub(crate) fn strip(line: &str) -> Option<&str> {
    for re in [&*CRI, &*COMPOSE, &*JOURNALD] {
        if let Some(caps) = re.captures(line) {
            return Some(caps.get(1).expect("group 1 exists").as_str());
        }
    }
    None
}
