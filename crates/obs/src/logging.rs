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

use rustfs_security_governance::{RedactionLevel, RedactionPolicyError, RedactionRule, validate_redaction_rules};
use std::fmt;

pub const REDACTED_LOG_VALUE: &str = "***redacted***";

pub const LOGGING_REDACTION_RULES: &[RedactionRule] = &[
    RedactionRule::new("access_key", RedactionLevel::Sensitive, "principal identifiers should be masked in logs"),
    RedactionRule::new(
        "authorization",
        RedactionLevel::Secret,
        "authorization headers may carry bearer or signature material",
    ),
    RedactionRule::new(
        "client_secret",
        RedactionLevel::Secret,
        "OIDC and external provider client secrets must never be logged",
    ),
    RedactionRule::new("secret_key", RedactionLevel::Secret, "secret access keys must never be logged"),
    RedactionRule::new(
        "session_token",
        RedactionLevel::Secret,
        "session tokens can be replayed if exposed in logs",
    ),
    RedactionRule::new("token", RedactionLevel::Secret, "generic token fields are treated as secret by default"),
];

pub fn validate_logging_redaction_rules() -> Result<(), RedactionPolicyError> {
    validate_redaction_rules(LOGGING_REDACTION_RULES)
}

pub fn is_sensitive_log_field(field_name: &str) -> bool {
    LOGGING_REDACTION_RULES
        .iter()
        .any(|rule| rule.field().eq_ignore_ascii_case(field_name.trim()) && rule.level().requires_redaction())
}

pub fn redacted_log_value(value: &str) -> &'static str {
    if value.is_empty() { "" } else { REDACTED_LOG_VALUE }
}

pub fn redacted_optional_log_value(value: Option<&str>) -> Option<&'static str> {
    value.map(redacted_log_value)
}

#[derive(Clone, Copy)]
pub struct MaskedAccessKey<'a>(pub &'a str);

impl fmt::Display for MaskedAccessKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0;
        if value.is_empty() {
            return Ok(());
        }

        let chars: Vec<char> = value.chars().collect();
        match chars.len() {
            0 => Ok(()),
            1..=4 => f.write_str("***"),
            5..=8 => write!(f, "{}***{}", chars[0], chars[chars.len() - 1]),
            len => {
                for ch in &chars[..4] {
                    write!(f, "{ch}")?;
                }
                f.write_str("***")?;
                for ch in &chars[len - 4..] {
                    write!(f, "{ch}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for MaskedAccessKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root should exist")
            .to_path_buf()
    }

    fn assert_no_unmasked_access_key_logging(rel_path: &str, forbidden_patterns: &[&str]) {
        let path = workspace_root().join(rel_path);
        let source = fs::read_to_string(&path).unwrap_or_else(|err| panic!("failed to read {}: {}", path.display(), err));
        for pattern in forbidden_patterns {
            assert!(
                !source.contains(pattern),
                "found forbidden unmasked access_key logging pattern `{}` in {}",
                pattern,
                path.display()
            );
        }
    }

    #[test]
    fn logging_redaction_rules_are_valid() {
        assert!(validate_logging_redaction_rules().is_ok());
    }

    #[test]
    fn identifies_sensitive_log_fields_case_insensitively() {
        assert!(is_sensitive_log_field("access_key"));
        assert!(is_sensitive_log_field("Authorization"));
        assert!(is_sensitive_log_field("session_token"));
        assert!(!is_sensitive_log_field("bucket"));
        assert!(!is_sensitive_log_field("status_code"));
    }

    #[test]
    fn redacted_log_value_preserves_empty_values() {
        assert_eq!(redacted_log_value(""), "");
        assert_eq!(redacted_log_value("secret"), REDACTED_LOG_VALUE);
        assert_eq!(redacted_optional_log_value(None), None);
        assert_eq!(redacted_optional_log_value(Some("")), Some(""));
        assert_eq!(redacted_optional_log_value(Some("secret")), Some(REDACTED_LOG_VALUE));
    }

    #[test]
    fn masked_access_key_masks_short_values() {
        assert_eq!(MaskedAccessKey("").to_string(), "");
        assert_eq!(MaskedAccessKey("a").to_string(), "***");
        assert_eq!(MaskedAccessKey("abcd").to_string(), "***");
        assert_eq!(MaskedAccessKey("abcde").to_string(), "a***e");
        assert_eq!(MaskedAccessKey("abcdefgh").to_string(), "a***h");
    }

    #[test]
    fn masked_access_key_masks_long_values() {
        assert_eq!(MaskedAccessKey("AKIAIOSFODNN7EXAMPLE").to_string(), "AKIA***MPLE");
        assert_eq!(format!("{:?}", MaskedAccessKey("keystone:user-1234")), "keys***1234");
    }

    #[test]
    fn runtime_auth_logging_does_not_use_previous_unmasked_access_key_patterns() {
        assert_no_unmasked_access_key_logging(
            "rustfs/src/auth.rs",
            &[
                "get_secret_key failed: no such user, access_key: {access_key}",
                "get_secret_key failed: check_key error, access_key: {access_key}, error: {e:?}",
                "get_secret_key failed: iam not initialized, access_key: {access_key}",
                "check_key_valid: user not found for access_key={}",
                "check_key_valid: account disabled for access_key={}",
                "check_key_valid: validation failed for access_key={}",
                "check_key_valid: starting validation - access_key={}, session_token_len={}",
            ],
        );
    }

    #[test]
    fn protocol_client_logging_does_not_use_previous_unmasked_access_key_pattern() {
        assert_no_unmasked_access_key_logging(
            "rustfs/src/protocols/client.rs",
            &["Protocol storage client ListBuckets request: access_key={}"],
        );
    }
}
