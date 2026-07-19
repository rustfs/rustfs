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

//! Built-in seed rule library (rustfs/backlog#1286), distilled from the
//! 2026-07 repository-wide failure-log survey.
//!
//! Every anchor string is verbatim source text (thiserror `#[error]`
//! displays or `error!`/`warn!` literals); the CI guard
//! (rustfs/backlog#1289) fails when a log message drifts away from its
//! rule. Titles/diagnoses/suggestions are operator-facing Chinese.

#[cfg(test)]
mod tests;

mod capacity;
mod disk;
mod erasure;
mod heal;
mod iam;
mod lock;
mod network;
mod ops;
mod process;
mod quorum;
mod scanner;
mod startup;

use super::model::{Matcher, Rule, Severity};

/// All built-in rules, grouped by category.
pub fn seed_rules() -> Vec<Rule> {
    let groups: [Vec<Rule>; 12] = [
        disk::rules(),
        erasure::rules(),
        quorum::rules(),
        network::rules(),
        lock::rules(),
        heal::rules(),
        scanner::rules(),
        iam::rules(),
        startup::rules(),
        capacity::rules(),
        ops::rules(),
        process::rules(),
    ];
    groups.into_iter().flatten().collect()
}

// -- shared construction helpers (pub(crate) within the seed tree) --

pub(crate) fn strings<const N: usize>(items: [&str; N]) -> Vec<String> {
    items.into_iter().map(String::from).collect()
}

pub(crate) fn prefix(text: &str) -> Matcher {
    Matcher::MessagePrefix(text.to_string())
}

pub(crate) fn contains(text: &str) -> Matcher {
    Matcher::MessageContains(text.to_string())
}

pub(crate) fn field(name: &str, value: &str) -> Matcher {
    Matcher::FieldEquals {
        name: name.to_string(),
        value: value.to_string(),
    }
}

pub(crate) fn any<const N: usize>(matchers: [Matcher; N]) -> Matcher {
    Matcher::Any(matchers.into())
}

pub(crate) fn all<const N: usize>(matchers: [Matcher; N]) -> Matcher {
    Matcher::All(matchers.into())
}

/// Base rule with empty extras; callers extend via struct update syntax.
pub(crate) fn base(
    id: &str,
    severity: Severity,
    category: &str,
    title: &str,
    matcher: Matcher,
    diagnosis: &str,
    suggestion: &str,
) -> Rule {
    Rule {
        id: id.to_string(),
        severity,
        category: category.to_string(),
        title: title.to_string(),
        matcher,
        diagnosis: diagnosis.to_string(),
        suggestion: suggestion.to_string(),
        evidence_fields: Vec::new(),
        min_count: 1,
        implies_root_cause: Vec::new(),
        anchors: Vec::new(),
    }
}
