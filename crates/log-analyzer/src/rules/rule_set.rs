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

//! Validated rule collection. Validation reports *all* problems at once,
//! not just the first — a rule author fixing an external file should not
//! have to iterate error by error.

use super::model::{Matcher, Rule};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuleSetError {
    #[error("duplicate rule id '{0}'")]
    DuplicateId(String),
    #[error("rule has an empty id")]
    EmptyId,
    #[error("rule '{rule}': bad regex: {source}")]
    BadRegex {
        rule: String,
        #[source]
        source: regex::Error,
    },
    #[error("rule '{0}': empty all/any matcher group")]
    EmptyGroup(String),
    #[error("rule '{rule}': bad anchor {anchor:?}: {reason}")]
    BadAnchor {
        rule: String,
        anchor: String,
        reason: &'static str,
    },
    #[error("{} rule set problem(s):\n{}", .0.len(), .0.iter().map(|e| format!("  - {e}")).collect::<Vec<_>>().join("\n"))]
    Multiple(Vec<RuleSetError>),
}

#[derive(Debug)]
pub struct RuleSet {
    rules: Vec<Rule>,
}

impl RuleSet {
    /// Validates: ids unique and non-empty, every `MessageRegex` compiles,
    /// every `All`/`Any` group is non-empty. Returns all violations.
    pub fn new(rules: Vec<Rule>) -> Result<Self, RuleSetError> {
        let mut problems = Vec::new();
        let mut seen = BTreeSet::new();
        for rule in &rules {
            if rule.id.trim().is_empty() {
                problems.push(RuleSetError::EmptyId);
            } else if !seen.insert(rule.id.clone()) {
                problems.push(RuleSetError::DuplicateId(rule.id.clone()));
            }
            validate_matcher(&rule.matcher, &rule.id, &mut problems);
            for anchor in &rule.anchors {
                // Anchors feed a TSV dump consumed line-by-line by the CI
                // guard (rustfs/backlog#1289); short anchors have no
                // discriminating power and would always grep-match.
                let reason = if anchor.contains('\t') || anchor.contains('\n') {
                    Some("must not contain tabs or newlines")
                } else if anchor.trim().is_empty() {
                    Some("must not be blank")
                } else if anchor.len() < 8 {
                    Some("must be at least 8 bytes")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    problems.push(RuleSetError::BadAnchor {
                        rule: rule.id.clone(),
                        anchor: anchor.clone(),
                        reason,
                    });
                }
            }
        }
        if problems.is_empty() {
            Ok(Self { rules })
        } else {
            Err(RuleSetError::Multiple(problems))
        }
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }
}

fn validate_matcher(matcher: &Matcher, rule_id: &str, problems: &mut Vec<RuleSetError>) {
    match matcher {
        Matcher::MessageRegex(re) => {
            if let Err(source) = regex::Regex::new(re) {
                problems.push(RuleSetError::BadRegex {
                    rule: rule_id.to_string(),
                    source,
                });
            }
        }
        Matcher::All(inner) | Matcher::Any(inner) => {
            if inner.is_empty() {
                problems.push(RuleSetError::EmptyGroup(rule_id.to_string()));
            }
            for m in inner {
                validate_matcher(m, rule_id, problems);
            }
        }
        _ => {}
    }
}
