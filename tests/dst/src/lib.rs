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

//! Deterministic simulation of RustFS 1.0.7 erasure sets.
//!
//! The harness drives the production layout selector, erasure codec, bitrot
//! hash, quorum reducer, and lock-quorum sizes. It does not boot the HTTP
//! server. See `tests/dst/README.md`.

pub mod bitrot;
pub mod campaign;
pub mod rng;
pub mod topology;
pub mod world;

pub use campaign::{
    CampaignConfig, CampaignReport, SeedFailure, SeedReport, render_report, run_official, run_seed, run_topologies,
};
pub use topology::{OFFICIAL_VOLUMES, Topology};
pub use world::{FaultRates, World, WorldConfig};
