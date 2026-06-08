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

use std::collections::BTreeSet;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedactionLevel {
    Public,
    Sensitive,
    Secret,
}

impl RedactionLevel {
    pub const fn requires_redaction(self) -> bool {
        matches!(self, Self::Sensitive | Self::Secret)
    }

    pub const fn is_secret(self) -> bool {
        matches!(self, Self::Secret)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RedactionRule {
    field: &'static str,
    level: RedactionLevel,
    reason: &'static str,
}

impl RedactionRule {
    pub const fn new(field: &'static str, level: RedactionLevel, reason: &'static str) -> Self {
        Self { field, level, reason }
    }

    pub const fn field(self) -> &'static str {
        self.field
    }

    pub const fn level(self) -> RedactionLevel {
        self.level
    }

    pub const fn reason(self) -> &'static str {
        self.reason
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RedactionPolicyError {
    #[error("redaction rule at index {index} has an empty field")]
    EmptyField { index: usize },

    #[error("redaction rule at index {index} for {field} has an empty reason")]
    EmptyReason { index: usize, field: &'static str },

    #[error("duplicate redaction rule for {field}")]
    DuplicateField { field: &'static str },
}

pub fn validate_redaction_rules(rules: &[RedactionRule]) -> Result<(), RedactionPolicyError> {
    let mut fields = BTreeSet::new();

    for (index, rule) in rules.iter().copied().enumerate() {
        if rule.field.trim().is_empty() {
            return Err(RedactionPolicyError::EmptyField { index });
        }

        if rule.reason.trim().is_empty() {
            return Err(RedactionPolicyError::EmptyReason {
                index,
                field: rule.field,
            });
        }

        if !fields.insert(rule.field) {
            return Err(RedactionPolicyError::DuplicateField { field: rule.field });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_redaction_rules() {
        let rules = [
            RedactionRule::new("access_key", RedactionLevel::Secret, "credential material"),
            RedactionRule::new("region", RedactionLevel::Public, "non-sensitive routing metadata"),
        ];

        assert!(validate_redaction_rules(&rules).is_ok());
        assert!(rules[0].level().requires_redaction());
        assert!(rules[0].level().is_secret());
        assert!(!rules[1].level().requires_redaction());
        assert_eq!(rules[0].field(), "access_key");
        assert_eq!(rules[0].reason(), "credential material");
    }

    #[test]
    fn rejects_empty_fields() {
        let rules = [RedactionRule::new(" ", RedactionLevel::Secret, "credential material")];

        let err = validate_redaction_rules(&rules).expect_err("empty field should fail validation");

        assert_eq!(err, RedactionPolicyError::EmptyField { index: 0 });
    }

    #[test]
    fn rejects_empty_reasons() {
        let rules = [RedactionRule::new("access_key", RedactionLevel::Secret, " ")];

        let err = validate_redaction_rules(&rules).expect_err("empty reason should fail validation");

        assert_eq!(
            err,
            RedactionPolicyError::EmptyReason {
                index: 0,
                field: "access_key"
            }
        );
    }

    #[test]
    fn rejects_duplicate_fields() {
        let rules = [
            RedactionRule::new("access_key", RedactionLevel::Secret, "credential material"),
            RedactionRule::new("access_key", RedactionLevel::Sensitive, "audit-only secret reference"),
        ];

        let err = validate_redaction_rules(&rules).expect_err("duplicate field should fail validation");

        assert_eq!(err, RedactionPolicyError::DuplicateField { field: "access_key" });
    }
}
