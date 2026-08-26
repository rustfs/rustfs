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

//! Heal-owned half of the scanner/heal overlap Phase-0 inventory (formerly a
//! cross-crate source include in crates/scanner). The assertions deliberately
//! check that the documented admission guards still exist; they do not claim
//! that a shared admission primitive already exists.

const HEAL_AUTO_SCAN_SOURCE: &str = include_str!("../src/heal/manager/auto_scan.rs");

#[test]
fn auto_scan_overlap_inventory_keeps_admission_guards() {
    assert!(HEAL_AUTO_SCAN_SOURCE.contains("active_heals"));
    assert!(HEAL_AUTO_SCAN_SOURCE.contains("contains_erasure_set"));
}
