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

//! External rule file loading (`--rules`, rustfs/backlog#1290 sub-item C):
//! the support team ships new rules — or hotfixes a misfiring built-in one —
//! without waiting for a release. External rules deserialize into the exact
//! same [`Rule`] type as the seed library (design decision 3 of
//! rustfs/backlog#1281); their anchors are NOT covered by the CI anchor
//! guard, so their quality is on the file author.

use super::model::Rule;
use super::rule_set::{RuleSet, RuleSetError};
use super::seed::seed_rules;
use serde::Deserialize;
use thiserror::Error;

/// Supported `schema_version` of the external rules file.
pub const EXTERNAL_RULES_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
struct ExternalRuleFile {
    schema_version: u32,
    rules: Vec<Rule>,
}

#[derive(Debug, Error)]
pub enum ExternalRulesError {
    #[error("cannot parse rules file: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("unsupported rules schema_version {found} (supported: {EXTERNAL_RULES_SCHEMA_VERSION})")]
    SchemaVersion { found: u32 },
    #[error("duplicate rule id '{0}' within the external rules file")]
    DuplicateInFile(String),
    #[error(transparent)]
    Invalid(#[from] RuleSetError),
}

/// Parses an external rule file and merges it over the built-in seed rules.
/// An external rule with the same id REPLACES the built-in one; the merged
/// set is validated as a whole and any problem fails the load — analysis
/// never runs with a half-broken rule set.
pub fn seed_rules_with_external(json: &str) -> Result<RuleSet, ExternalRulesError> {
    let file: ExternalRuleFile = serde_json::from_str(json)?;
    if file.schema_version != EXTERNAL_RULES_SCHEMA_VERSION {
        return Err(ExternalRulesError::SchemaVersion {
            found: file.schema_version,
        });
    }
    // Same-id merging would silently swallow duplicates inside the file
    // itself, so reject those before merging.
    let mut seen = std::collections::BTreeSet::new();
    for rule in &file.rules {
        if !seen.insert(rule.id.clone()) {
            return Err(ExternalRulesError::DuplicateInFile(rule.id.clone()));
        }
    }

    let mut rules = seed_rules();
    for external in file.rules {
        match rules.iter_mut().find(|rule| rule.id == external.id) {
            Some(existing) => *existing = external,
            None => rules.push(external),
        }
    }
    Ok(RuleSet::new(rules)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventKind, LogEvent, LogLevel, SourceRef};
    use crate::rules::RuleEngine;
    use std::sync::Arc;

    fn event(message: &str) -> LogEvent {
        LogEvent {
            timestamp: None,
            level: Some(LogLevel::Error),
            target: None,
            message: message.to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::from("rustfs.log"),
                line: 1,
            },
            node: None,
            kind: EventKind::Json,
        }
    }

    /// The canonical external-file shape; also documents the on-disk format
    /// (kept in sync with docs/operations/log-diagnose.md).
    const EXTERNAL_FILE: &str = r#"{
      "schema_version": 1,
      "rules": [
        {
          "id": "custom-oom-killer",
          "severity": "p1_unavailable",
          "category": "process",
          "title": "内核 OOM killer 终止了进程",
          "matcher": { "message_contains": "Out of memory: Killed process" },
          "diagnosis": "内核因内存不足杀掉了 rustfs 进程。",
          "suggestion": "检查内存限制与其他驻留进程;考虑调大内存或加节点。"
        }
      ]
    }"#;

    #[test]
    fn external_rule_is_added_and_matches() {
        let set = seed_rules_with_external(EXTERNAL_FILE).expect("load");
        let seed_len = seed_rules().len();
        assert_eq!(set.rules().len(), seed_len + 1);
        let engine = RuleEngine::new(set);
        let hits = engine.matches(&event("Out of memory: Killed process 1234 (rustfs)"));
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn same_id_overrides_the_builtin_rule() {
        let file = r#"{
          "schema_version": 1,
          "rules": [
            {
              "id": "ec-write-quorum",
              "severity": "p4_info",
              "category": "erasure",
              "title": "覆盖后的写仲裁规则",
              "matcher": { "message_contains": "OVERRIDDEN-ANCHOR-TEXT" },
              "diagnosis": "外部覆盖。",
              "suggestion": "外部覆盖。"
            }
          ]
        }"#;
        let set = seed_rules_with_external(file).expect("load");
        assert_eq!(set.rules().len(), seed_rules().len(), "override must not grow the set");
        let engine = RuleEngine::new(set);
        // The built-in matcher no longer fires; the external one does.
        assert!(
            engine
                .matches(&event("erasure write quorum (required=8, achieved=5)"))
                .is_empty()
        );
        assert_eq!(engine.matches(&event("OVERRIDDEN-ANCHOR-TEXT hit")).len(), 1);
    }

    #[test]
    fn bad_files_fail_fast_with_actionable_errors() {
        // Not JSON at all.
        assert!(matches!(seed_rules_with_external("nope"), Err(ExternalRulesError::Parse(_))));

        // Wrong schema version.
        let err = seed_rules_with_external(r#"{"schema_version": 9, "rules": []}"#).expect_err("schema");
        assert!(err.to_string().contains("schema_version 9"), "{err}");

        // Duplicate id inside the file.
        let dup = r#"{
          "schema_version": 1,
          "rules": [
            {"id": "x-dup", "severity": "p4_info", "category": "ops", "title": "a",
             "matcher": {"message_contains": "aaaaaaaaaa"}, "diagnosis": "d", "suggestion": "s"},
            {"id": "x-dup", "severity": "p4_info", "category": "ops", "title": "b",
             "matcher": {"message_contains": "bbbbbbbbbb"}, "diagnosis": "d", "suggestion": "s"}
          ]
        }"#;
        let err = seed_rules_with_external(dup).expect_err("dup");
        assert!(err.to_string().contains("x-dup"), "{err}");

        // Bad regex: the merged-set validation names the offending rule.
        let bad_regex = r#"{
          "schema_version": 1,
          "rules": [
            {"id": "x-bad-regex", "severity": "p4_info", "category": "ops", "title": "a",
             "matcher": {"message_regex": "[unclosed"}, "diagnosis": "d", "suggestion": "s"}
          ]
        }"#;
        let err = seed_rules_with_external(bad_regex).expect_err("regex");
        assert!(err.to_string().contains("x-bad-regex"), "{err}");
    }
}
