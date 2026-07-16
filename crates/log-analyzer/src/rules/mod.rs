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

//! Rule model, matching engine, and finding aggregation
//! (rustfs/backlog#1285). The built-in seed rule library is LA-5
//! (rustfs/backlog#1286).

mod engine;
mod findings;
mod model;
mod rule_set;
mod seed;

pub use engine::RuleEngine;
pub use findings::{EvidenceValues, Finding, FindingsCollector};
pub use model::{Matcher, Rule, Severity};
pub use rule_set::{RuleSet, RuleSetError};
pub use seed::seed_rules;

/// The built-in rule library as a validated set. Seed rules are validated
/// by construction; a failure here is a bug in the seed tree itself.
pub fn seed_rule_set() -> RuleSet {
    RuleSet::new(seed_rules()).expect("seed rules must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventKind, LogEvent, LogLevel, SourceRef};
    use std::sync::Arc;

    fn event(message: &str, level: Option<LogLevel>, line: u64) -> LogEvent {
        LogEvent {
            timestamp: None,
            level,
            target: Some("rustfs::scanner::io".to_string()),
            message: message.to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::from("rustfs.log"),
                line,
            },
            node: None,
            kind: EventKind::Json,
        }
    }

    fn event_at(message: &str, ts: &str, line: u64) -> LogEvent {
        LogEvent {
            timestamp: Some(chrono::DateTime::parse_from_rfc3339(ts).expect("ts")),
            ..event(message, Some(LogLevel::Error), line)
        }
    }

    fn rule(id: &str, severity: Severity, matcher: Matcher) -> Rule {
        Rule {
            id: id.to_string(),
            severity,
            category: "disk".to_string(),
            title: format!("title {id}"),
            matcher,
            diagnosis: "diag".to_string(),
            suggestion: "fix".to_string(),
            evidence_fields: Vec::new(),
            min_count: 1,
            implies_root_cause: Vec::new(),
            anchors: Vec::new(),
        }
    }

    fn engine_of(rules: Vec<Rule>) -> RuleEngine {
        RuleEngine::new(RuleSet::new(rules).expect("valid rules"))
    }

    #[test]
    fn every_matcher_kind_has_positive_and_negative_cases() {
        let ev = {
            let mut ev = event("erasure write quorum (required=8, achieved=5)", Some(LogLevel::Error), 1);
            ev.fields
                .insert("reason".to_string(), serde_json::Value::String("faulty_disk".into()));
            ev.fields.insert("achieved".to_string(), serde_json::json!(5));
            ev
        };
        let panic_ev = LogEvent {
            kind: EventKind::Panic,
            ..event("thread 'main' panicked at src/main.rs:3:5: boom", Some(LogLevel::Error), 2)
        };

        let cases: Vec<(Matcher, bool, bool)> = vec![
            // (matcher, matches ev, matches panic_ev)
            (Matcher::MessagePrefix("erasure write quorum (".into()), true, false),
            (Matcher::MessageContains("achieved=5".into()), true, false),
            (Matcher::MessageRegex(r"required=\d+".into()), true, false),
            (
                Matcher::FieldEquals {
                    name: "reason".into(),
                    value: "faulty_disk".into(),
                },
                true,
                false,
            ),
            (Matcher::TargetPrefix("rustfs::scanner".into()), true, true),
            (Matcher::IsPanic, false, true),
            (Matcher::MinLevel(LogLevel::Warn), true, true),
            (
                Matcher::All(vec![
                    Matcher::MinLevel(LogLevel::Error),
                    Matcher::Any(vec![Matcher::IsPanic, Matcher::MessageContains("quorum".into())]),
                ]),
                true,
                true,
            ),
            (
                Matcher::Any(vec![Matcher::IsPanic, Matcher::MessagePrefix("no such".into())]),
                false,
                true,
            ),
        ];
        for (matcher, expect_ev, expect_panic) in cases {
            let engine = engine_of(vec![rule("r", Severity::P2Degraded, matcher.clone())]);
            assert_eq!(engine.matches(&ev).len() == 1, expect_ev, "matcher {matcher:?} vs ev");
            assert_eq!(engine.matches(&panic_ev).len() == 1, expect_panic, "matcher {matcher:?} vs panic");
        }
    }

    #[test]
    fn field_equals_coerces_numbers_via_display() {
        let mut ev = event("x", Some(LogLevel::Error), 1);
        ev.fields.insert("achieved".to_string(), serde_json::json!(1));
        let engine = engine_of(vec![rule(
            "n",
            Severity::P1Unavailable,
            Matcher::FieldEquals {
                name: "achieved".into(),
                value: "1".into(),
            },
        )]);
        assert_eq!(engine.matches(&ev).len(), 1);

        // A JSON string "1" also matches (field_display unquotes strings)…
        ev.fields.insert("achieved".to_string(), serde_json::json!("1"));
        assert_eq!(engine.matches(&ev).len(), 1);
        // …but a quoted value does not accidentally match.
        ev.fields.insert("achieved".to_string(), serde_json::json!("\"1\""));
        assert_eq!(engine.matches(&ev).len(), 0);
    }

    #[test]
    fn min_level_requires_a_level() {
        let engine = engine_of(vec![rule("lvl", Severity::P4Info, Matcher::MinLevel(LogLevel::Warn))]);
        assert_eq!(engine.matches(&event("x", Some(LogLevel::Error), 1)).len(), 1);
        assert_eq!(engine.matches(&event("x", Some(LogLevel::Info), 1)).len(), 0);
        assert_eq!(engine.matches(&event("x", None, 1)).len(), 0);
    }

    #[test]
    fn rule_set_reports_all_problems_at_once() {
        let rules = vec![
            rule("dup", Severity::P4Info, Matcher::IsPanic),
            rule("dup", Severity::P4Info, Matcher::MessageRegex("[unclosed".into())),
            rule("", Severity::P4Info, Matcher::All(vec![])),
        ];
        let err = RuleSet::new(rules).expect_err("invalid");
        let RuleSetError::Multiple(problems) = err else {
            panic!("expected Multiple");
        };
        assert_eq!(problems.len(), 4, "{problems:?}"); // dup id + bad regex + empty id + empty group
    }

    #[test]
    fn one_event_can_hit_several_rules() {
        let engine = engine_of(vec![
            rule("a", Severity::P2Degraded, Matcher::MessageContains("quorum".into())),
            rule("b", Severity::P1Unavailable, Matcher::MinLevel(LogLevel::Error)),
        ]);
        let hits = engine.matches(&event("write quorum lost", Some(LogLevel::Error), 1));
        assert_eq!(hits, vec![0, 1]);
    }

    #[test]
    fn collector_is_order_independent() {
        let r = rule("q", Severity::P1Unavailable, Matcher::MessageContains("quorum".into()));
        let events = vec![
            event_at("quorum e1", "2026-07-15T03:00:00+08:00", 10),
            event_at("quorum e2", "2026-07-15T01:00:00+08:00", 20),
            event_at("quorum e3", "2026-07-15T02:00:00+08:00", 30),
            event_at("quorum e4", "2026-07-15T04:00:00+08:00", 40),
            event("quorum no-ts", Some(LogLevel::Error), 50),
        ];

        let collect = |order: &[usize]| {
            let mut collector = FindingsCollector::new(3);
            for &i in order {
                collector.observe(&r, 0, &events[i]);
            }
            serde_json::to_string(&collector.into_findings()).expect("json")
        };

        let forward = collect(&[0, 1, 2, 3, 4]);
        let reverse = collect(&[4, 3, 2, 1, 0]);
        let shuffled = collect(&[2, 4, 0, 3, 1]);
        assert_eq!(forward, reverse);
        assert_eq!(forward, shuffled);

        let findings: Vec<Finding> = {
            let mut collector = FindingsCollector::new(3);
            for ev in &events {
                collector.observe(&r, 0, ev);
            }
            collector.into_findings()
        };
        let f = &findings[0];
        assert_eq!(f.count, 5);
        assert_eq!(f.first_seen.expect("first").to_rfc3339(), "2026-07-15T01:00:00+08:00");
        assert_eq!(f.last_seen.expect("last").to_rfc3339(), "2026-07-15T04:00:00+08:00");
        let sample_messages: Vec<_> = f.samples.iter().map(|s| s.message.as_str()).collect();
        assert_eq!(sample_messages, vec!["quorum e2", "quorum e3", "quorum e1"]);
    }

    #[test]
    fn sample_ties_across_files_are_order_independent() {
        let r = rule("q", Severity::P1Unavailable, Matcher::MessageContains("quorum".into()));
        // Same timestamp and same line number in two different files: only
        // the file name can break the tie deterministically.
        let mk = |file: &str, msg: &str| {
            let mut ev = event_at(msg, "2026-07-15T03:00:00+08:00", 1);
            ev.source.file = Arc::from(file);
            ev
        };
        let a = mk("node1/rustfs.log", "quorum from node1");
        let b = mk("node2/rustfs.log", "quorum from node2");

        let selected = |events: &[&LogEvent]| {
            let mut collector = FindingsCollector::new(1);
            for ev in events {
                collector.observe(&r, 0, ev);
            }
            collector.into_findings()[0].samples[0].message.clone()
        };
        assert_eq!(selected(&[&a, &b]), selected(&[&b, &a]));
        assert_eq!(selected(&[&a, &b]), "quorum from node1");
    }

    #[test]
    fn evidence_values_are_capped_with_overflow() {
        let mut r = rule("e", Severity::P2Degraded, Matcher::MessageContains("x".into()));
        r.evidence_fields = vec!["disk".to_string()];
        let mut collector = FindingsCollector::new(3);
        for i in 0..11 {
            let mut ev = event("x", Some(LogLevel::Error), i);
            ev.fields
                .insert("disk".to_string(), serde_json::Value::String(format!("/data/disk{i}")));
            collector.observe(&r, 0, &ev);
        }
        let findings = collector.into_findings();
        let evidence = &findings[0].evidence["disk"];
        assert_eq!(evidence.values.len(), 10);
        assert_eq!(evidence.overflow, 1);
    }

    #[test]
    fn below_min_count_is_flagged() {
        let mut r = rule("m", Severity::P3ClientSide, Matcher::MessageContains("denied".into()));
        r.min_count = 5;
        let mut collector = FindingsCollector::new(3);
        for i in 0..3 {
            collector.observe(&r, 0, &event("denied", Some(LogLevel::Warn), i));
        }
        assert!(collector.into_findings()[0].below_min_count);
    }

    #[test]
    fn findings_sort_by_severity_then_count() {
        let rules = [
            rule("p2", Severity::P2Degraded, Matcher::MessageContains("a".into())),
            rule("p0", Severity::P0DataRisk, Matcher::MessageContains("b".into())),
            rule("p1-low", Severity::P1Unavailable, Matcher::MessageContains("c".into())),
            rule("p1-high", Severity::P1Unavailable, Matcher::MessageContains("d".into())),
        ];
        let mut collector = FindingsCollector::new(3);
        let feed = |collector: &mut FindingsCollector, idx: usize, msg: &str, times: u64| {
            for i in 0..times {
                collector.observe(&rules[idx], idx, &event(msg, Some(LogLevel::Error), i));
            }
        };
        feed(&mut collector, 0, "a", 100);
        feed(&mut collector, 1, "b", 1);
        feed(&mut collector, 2, "c", 2);
        feed(&mut collector, 3, "d", 9);
        let ids: Vec<_> = collector.into_findings().into_iter().map(|f| f.rule_id).collect();
        assert_eq!(ids, vec!["p0", "p1-high", "p1-low", "p2"]);
    }

    #[test]
    fn rule_json_round_trip_and_schema_shape() {
        let json = r#"{
          "id": "demo-rule",
          "severity": "p1_unavailable",
          "category": "disk",
          "title": "示例",
          "matcher": { "all": [ { "message_prefix": "erasure write quorum (" }, { "min_level": "WARN" } ] },
          "diagnosis": "…",
          "suggestion": "…",
          "evidence_fields": ["required", "achieved"],
          "anchors": ["erasure write quorum ("]
        }"#;
        let parsed: Rule = serde_json::from_str(json).expect("hand-written JSON stays parseable");
        assert_eq!(parsed.id, "demo-rule");
        assert_eq!(parsed.severity, Severity::P1Unavailable);
        assert_eq!(parsed.min_count, 1, "min_count defaults");
        assert!(matches!(&parsed.matcher, Matcher::All(inner) if inner.len() == 2));

        let round: Rule = serde_json::from_str(&serde_json::to_string(&parsed).expect("ser")).expect("de");
        assert_eq!(serde_json::to_value(&round).expect("v"), serde_json::to_value(&parsed).expect("v"));
    }
}
