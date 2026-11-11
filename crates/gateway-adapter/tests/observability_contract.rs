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

//! The legacy observability contract and who owns each item of it under the gateway.
//!
//! Responsible for: that `tests/fixtures/observability-contract.json` still names what the legacy
//! code emits (every item's literal is on the line it cites), that every item has exactly one known
//! owner, and that the items the fixture gives to this crate's two modules are exactly the ones those
//! modules declare.
//! NOT responsible for: values. The contract is one of names.
//! Upstream: the fixture and the RustFS sources it cites. Downstream: nothing.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use rustfs_gateway_adapter::{authz_audit, observer};
use serde_json::Value;

const OBSERVER: &str = "gateway-observer";
const AUTHZ_AUDIT: &str = "gateway-authz-audit";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the crate lives at crates/<name>")
        .to_path_buf()
}

fn contract() -> Value {
    let text = include_str!("fixtures/observability-contract.json");
    serde_json::from_str(text).expect("the contract is JSON")
}

fn items(contract: &Value) -> &Vec<Value> {
    contract["items"].as_array().expect("an item list")
}

fn field<'a>(item: &'a Value, name: &str) -> &'a str {
    item[name]
        .as_str()
        .unwrap_or_else(|| panic!("item {item} has no string `{name}`"))
}

fn owned_by<'a>(contract: &'a Value, owner: &str) -> BTreeSet<&'a str> {
    items(contract)
        .iter()
        .filter(|item| field(item, "owner") == owner)
        .map(|item| field(item, "id"))
        .collect()
}

/// Negative — an item without a known owner, or with a duplicate id, fails: nothing in the contract
/// may be left unowned or owned twice.
#[test]
fn contract_every_item_has_one_known_owner() {
    let contract = contract();
    let owners: HashSet<&str> = contract["owners"]
        .as_object()
        .expect("an owner vocabulary")
        .keys()
        .map(String::as_str)
        .collect();
    assert!(owners.contains(OBSERVER) && owners.contains(AUTHZ_AUDIT), "{owners:?}");
    let mut ids = HashSet::new();
    for item in items(&contract) {
        let id = field(item, "id");
        assert!(ids.insert(id), "duplicate contract item {id}");
        assert!(owners.contains(field(item, "owner")), "{id} has an unknown owner");
        assert!(
            matches!(field(item, "kind"), "metric" | "label" | "audit_field"),
            "{id} has an unknown kind"
        );
    }
    let metrics = items(&contract).iter().filter(|item| field(item, "kind") == "metric").count();
    let labels = items(&contract).iter().filter(|item| field(item, "kind") == "label").count();
    let fields = items(&contract)
        .iter()
        .filter(|item| field(item, "kind") == "audit_field")
        .count();
    assert_eq!(
        (metrics, labels, fields),
        (19, 19, 48),
        "the contract changed size; update the PR's counts"
    );
}

/// Negative — a contract item whose cited line no longer carries its literal is stale, which is how a
/// silent rename on the legacy side would show up here.
#[test]
fn contract_every_item_is_where_it_says() {
    let root = workspace_root();
    let contract = contract();
    for item in items(&contract) {
        let id = field(item, "id");
        let (path, line) = field(item, "emitted_at").rsplit_once(':').expect("path:line");
        let line: usize = line.parse().expect("a line number");
        let text = std::fs::read_to_string(root.join(path)).unwrap_or_else(|error| panic!("{id}: {path}: {error}"));
        let cited = text
            .lines()
            .nth(line - 1)
            .unwrap_or_else(|| panic!("{id}: {path} has no line {line}"));
        let literal = field(item, "literal");
        assert!(cited.contains(literal), "{id}: {path}:{line} no longer contains {literal:?}: {cited}");
    }
}

/// Negative — the fixture's observer-owned items are exactly what `observer` declares it supplies.
#[test]
fn contract_observer_items_match_the_module() {
    let contract = contract();
    let declared: BTreeSet<&str> = observer::OWNED_CONTRACT_ITEMS.into_iter().collect();
    assert_eq!(owned_by(&contract, OBSERVER), declared);
}

/// Negative — the fixture's audit-owned fields are exactly what `authz_audit` declares, and every one
/// is a field of the legacy authorization-denial entry.
#[test]
fn contract_authz_audit_fields_match_the_module() {
    let contract = contract();
    let declared: BTreeSet<String> = authz_audit::OWNED_CONTRACT_FIELDS
        .into_iter()
        .map(|field| format!("authorization_denial.{field}"))
        .collect();
    let owned: BTreeSet<String> = owned_by(&contract, AUTHZ_AUDIT).into_iter().map(str::to_owned).collect();
    assert_eq!(owned, declared);
}
