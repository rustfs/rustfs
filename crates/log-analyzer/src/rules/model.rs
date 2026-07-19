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

//! Rule data model.
//!
//! Rules are owned values (not `&'static` graphs) on purpose: the Phase-2
//! external rule file deserializes into these exact types with zero
//! impedance (see rustfs/backlog#1281 design decision 3).

use crate::model::LogLevel;
use serde::{Deserialize, Serialize};

/// Severity == what the operator should do about it. `Ord` puts the most
/// severe first, which is the report order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// P0: possible data loss / unreconstructable objects.
    P0DataRisk,
    /// P1: service unavailable — cannot write, cannot start, process crash.
    P1Unavailable,
    /// P2: degraded — self-healing, partial disk/node failure, still serving.
    P2Degraded,
    /// P3: client/environment side — credentials, quota, policy.
    P3ClientSide,
    /// P4: informational — state-machine conflicts, ignorable noise.
    P4Info,
}

impl Severity {
    /// Human label used by report renderers.
    pub fn label(&self) -> &'static str {
        match self {
            Severity::P0DataRisk => "P0 数据风险",
            Severity::P1Unavailable => "P1 服务不可用",
            Severity::P2Degraded => "P2 降级",
            Severity::P3ClientSide => "P3 客户端侧",
            Severity::P4Info => "P4 提示",
        }
    }
}

/// Match condition tree. Semantics (see rustfs/backlog#1285):
///
/// - level `None` never satisfies [`Matcher::MinLevel`];
/// - target `None` never satisfies [`Matcher::TargetPrefix`];
/// - [`Matcher::FieldEquals`] compares via `LogEvent::field_display`, so
///   the numeric field `{"n":3}` equals the string value `"3"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Matcher {
    /// `event.message` starts with the given text.
    MessagePrefix(String),
    /// `event.message` contains the given text.
    MessageContains(String),
    /// Regex over `event.message`. Compilation is validated at
    /// `RuleSet::new` time (fail fast), not per event.
    MessageRegex(String),
    /// `fields[name]` rendered via `field_display` equals `value`.
    FieldEquals {
        name: String,
        value: String,
    },
    /// `event.target` starts with the given prefix.
    TargetPrefix(String),
    /// `event.kind == EventKind::Panic`.
    IsPanic,
    /// `event.level >= level` (`None` level never matches).
    MinLevel(LogLevel),
    All(Vec<Matcher>),
    Any(Vec<Matcher>),
}

fn default_min_count() -> u64 {
    1
}

/// One diagnosis rule. Text fields are operator-facing Chinese; `id` and
/// `category` are stable kebab-case identifiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Unique kebab-case id, e.g. "ec-write-quorum".
    pub id: String,
    pub severity: Severity,
    /// Grouping: "disk" | "erasure" | "quorum" | "network" | "lock" | "heal"
    /// | "scanner" | "iam" | "startup" | "capacity" | "ops" | "process".
    pub category: String,
    /// One-line report title.
    pub title: String,
    pub matcher: Matcher,
    /// What this pattern usually means (1-3 sentences).
    pub diagnosis: String,
    /// Actionable advice (1-3 sentences).
    pub suggestion: String,
    /// Field names collected from matching events as evidence.
    #[serde(default)]
    pub evidence_fields: Vec<String>,
    /// Findings with fewer hits are demoted to the low-confidence section.
    #[serde(default = "default_min_count")]
    pub min_count: u64,
    /// Phase-2 root-cause folding (rustfs/backlog#1290); stored only for now.
    #[serde(default)]
    pub implies_root_cause: Vec<String>,
    /// CI guard (rustfs/backlog#1289): these texts must exist verbatim in
    /// the rustfs source tree. Rules matching on message text carry at
    /// least one anchor; pure field/panic rules may have none.
    #[serde(default)]
    pub anchors: Vec<String>,
}
