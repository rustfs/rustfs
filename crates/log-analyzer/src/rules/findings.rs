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

//! Finding aggregation. Every accumulator is a commutative monoid
//! (count/min/max/set-union), so results are byte-identical no matter the
//! input order — the order-independence contract of rustfs/backlog#1281.

use super::model::{Rule, Severity};
use crate::model::LogEvent;
use chrono::{DateTime, FixedOffset};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const EVIDENCE_VALUES_CAP: usize = 10;

/// Deduplicated evidence values for one field, capped with an overflow count.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EvidenceValues {
    pub values: BTreeSet<String>,
    /// Occurrences of values that did not fit the cap (not deduplicated —
    /// remembering overflow values would defeat the memory bound).
    pub overflow: u64,
}

/// One rule's aggregated hits across the whole input.
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub rule_id: String,
    pub severity: Severity,
    pub category: String,
    pub title: String,
    pub diagnosis: String,
    pub suggestion: String,
    pub count: u64,
    /// Earliest / latest timestamps among hits that carry one.
    pub first_seen: Option<DateTime<FixedOffset>>,
    pub last_seen: Option<DateTime<FixedOffset>>,
    /// Node labels of hits; label-less events are recorded as "-".
    pub nodes: BTreeSet<String>,
    /// The earliest `max_samples` hit events (timestamp-less ones sort last).
    pub samples: Vec<LogEvent>,
    /// evidence field name -> deduplicated values.
    pub evidence: BTreeMap<String, EvidenceValues>,
    /// `count < rule.min_count`: the report demotes this to low-confidence.
    pub below_min_count: bool,
    /// Causal folding (rustfs/backlog#1290): the root-cause finding this one
    /// collapsed into. Renderers fold it under that root instead of listing
    /// it flat; JSON keeps the full finding either way.
    pub collapsed_into: Option<String>,
    /// The symptom finding ids collapsed under this root (other direction).
    pub caused: Vec<String>,
    /// The rule's causal edges, carried for `finalize` — not report data.
    #[serde(skip)]
    pub implies_root_cause: Vec<String>,
}

struct FindingAcc {
    finding: Finding,
    min_count: u64,
    evidence_fields: Vec<String>,
}

pub struct FindingsCollector {
    max_samples: usize,
    accs: BTreeMap<usize, FindingAcc>,
}

/// Sort key for sample selection: earliest first, timestamp-less last,
/// (file, line) as the deterministic tiebreaker — line numbers alone are only
/// unique within one file, so ties across inputs would be order-dependent.
fn sample_key(event: &LogEvent) -> (bool, Option<DateTime<FixedOffset>>, &str, u64) {
    (event.timestamp.is_none(), event.timestamp, &event.source.file, event.source.line)
}

/// `DateTime<FixedOffset>` orders by instant, so two equal instants carrying
/// different offsets compare equal and `min`/`max` keep whichever arrived
/// first — an input-order dependency that then leaks into the serialized
/// RFC3339 offset. Break the tie on the offset itself to stay deterministic.
fn earliest(a: DateTime<FixedOffset>, b: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    match a.cmp(&b) {
        std::cmp::Ordering::Less => a,
        std::cmp::Ordering::Greater => b,
        std::cmp::Ordering::Equal if a.offset().local_minus_utc() <= b.offset().local_minus_utc() => a,
        std::cmp::Ordering::Equal => b,
    }
}

fn latest(a: DateTime<FixedOffset>, b: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    match a.cmp(&b) {
        std::cmp::Ordering::Greater => a,
        std::cmp::Ordering::Less => b,
        std::cmp::Ordering::Equal if a.offset().local_minus_utc() >= b.offset().local_minus_utc() => a,
        std::cmp::Ordering::Equal => b,
    }
}

impl FindingsCollector {
    pub fn new(max_samples: usize) -> Self {
        Self {
            max_samples,
            accs: BTreeMap::new(),
        }
    }

    pub fn observe(&mut self, rule: &Rule, rule_idx: usize, event: &LogEvent) {
        let acc = self.accs.entry(rule_idx).or_insert_with(|| FindingAcc {
            finding: Finding {
                rule_id: rule.id.clone(),
                severity: rule.severity,
                category: rule.category.clone(),
                title: rule.title.clone(),
                diagnosis: rule.diagnosis.clone(),
                suggestion: rule.suggestion.clone(),
                count: 0,
                first_seen: None,
                last_seen: None,
                nodes: BTreeSet::new(),
                samples: Vec::new(),
                evidence: BTreeMap::new(),
                below_min_count: false,
                collapsed_into: None,
                caused: Vec::new(),
                implies_root_cause: rule.implies_root_cause.clone(),
            },
            min_count: rule.min_count,
            evidence_fields: rule.evidence_fields.clone(),
        });

        let finding = &mut acc.finding;
        finding.count += 1;

        if let Some(ts) = event.timestamp {
            finding.first_seen = Some(finding.first_seen.map_or(ts, |cur| earliest(cur, ts)));
            finding.last_seen = Some(finding.last_seen.map_or(ts, |cur| latest(cur, ts)));
        }

        finding.nodes.insert(event.node.as_deref().unwrap_or("-").to_string());

        // Bounded, order-independent sample selection: keep the earliest
        // `max_samples` events by (has-ts, ts, line).
        if self.max_samples > 0 {
            let key = sample_key(event);
            let full = finding.samples.len() >= self.max_samples;
            if !full || key < sample_key(finding.samples.last().expect("non-empty when full")) {
                let pos = finding.samples.partition_point(|s| sample_key(s) <= key);
                finding.samples.insert(pos, event.clone());
                finding.samples.truncate(self.max_samples);
            }
        }

        for field in &acc.evidence_fields {
            if let Some(value) = event.field_display(field) {
                let slot = finding.evidence.entry(field.clone()).or_default();
                if slot.values.contains(&value) {
                    continue;
                }
                slot.values.insert(value);
                // Keep a deterministic subset — the lexicographically smallest
                // EVIDENCE_VALUES_CAP distinct values — so the evidence set is
                // order-independent (previously it kept the first-10-by-arrival).
                if slot.values.len() > EVIDENCE_VALUES_CAP {
                    if let Some(max) = slot.values.iter().next_back().cloned() {
                        slot.values.remove(&max);
                    }
                    slot.overflow += 1;
                }
            }
        }
    }

    /// Sorted: severity ascending (P0 first) -> count descending -> id.
    pub fn into_findings(self) -> Vec<Finding> {
        let mut findings: Vec<Finding> = self
            .accs
            .into_values()
            .map(|acc| {
                let mut finding = acc.finding;
                finding.below_min_count = finding.count < acc.min_count;
                finding
            })
            .collect();
        findings.sort_by(|a, b| {
            a.severity
                .cmp(&b.severity)
                .then(b.count.cmp(&a.count))
                .then(a.rule_id.cmp(&b.rule_id))
        });
        findings
    }
}
