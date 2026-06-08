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
pub enum UnknownFieldPolicy {
    Deny,
    Warn,
    Preserve,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SerdePolicyKind {
    StrictIngress,
    TolerantCompat,
    PersistentLegacy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SerdePolicy {
    target: &'static str,
    kind: SerdePolicyKind,
    unknown_fields: UnknownFieldPolicy,
}

impl SerdePolicy {
    pub const fn new(target: &'static str, kind: SerdePolicyKind, unknown_fields: UnknownFieldPolicy) -> Self {
        Self {
            target,
            kind,
            unknown_fields,
        }
    }

    pub const fn target(self) -> &'static str {
        self.target
    }

    pub const fn kind(self) -> SerdePolicyKind {
        self.kind
    }

    pub const fn unknown_fields(self) -> UnknownFieldPolicy {
        self.unknown_fields
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SerdePolicyError {
    #[error("serde policy at index {index} has an empty target")]
    EmptyTarget { index: usize },

    #[error("strict ingress serde policy for {target} must deny unknown fields")]
    StrictIngressMustDeny { target: &'static str },

    #[error("compat serde policy for {target} must not deny unknown fields")]
    CompatMustNotDeny { target: &'static str },

    #[error("duplicate serde policy for {target}")]
    DuplicateTarget { target: &'static str },
}

pub fn validate_serde_policies(policies: &[SerdePolicy]) -> Result<(), SerdePolicyError> {
    let mut targets = BTreeSet::new();

    for (index, policy) in policies.iter().copied().enumerate() {
        if policy.target.trim().is_empty() {
            return Err(SerdePolicyError::EmptyTarget { index });
        }

        match (policy.kind, policy.unknown_fields) {
            (SerdePolicyKind::StrictIngress, UnknownFieldPolicy::Deny) => {}
            (SerdePolicyKind::StrictIngress, _) => {
                return Err(SerdePolicyError::StrictIngressMustDeny { target: policy.target });
            }
            (SerdePolicyKind::TolerantCompat | SerdePolicyKind::PersistentLegacy, UnknownFieldPolicy::Deny) => {
                return Err(SerdePolicyError::CompatMustNotDeny { target: policy.target });
            }
            (SerdePolicyKind::TolerantCompat | SerdePolicyKind::PersistentLegacy, _) => {}
        }

        if !targets.insert(policy.target) {
            return Err(SerdePolicyError::DuplicateTarget { target: policy.target });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_strict_and_compat_policies() {
        let policies = [
            SerdePolicy::new("BucketPolicy", SerdePolicyKind::StrictIngress, UnknownFieldPolicy::Deny),
            SerdePolicy::new("LegacyConfig", SerdePolicyKind::PersistentLegacy, UnknownFieldPolicy::Preserve),
            SerdePolicy::new("ReplicationRule", SerdePolicyKind::TolerantCompat, UnknownFieldPolicy::Warn),
        ];

        assert!(validate_serde_policies(&policies).is_ok());
        assert_eq!(policies[0].target(), "BucketPolicy");
        assert_eq!(policies[0].kind(), SerdePolicyKind::StrictIngress);
        assert_eq!(policies[0].unknown_fields(), UnknownFieldPolicy::Deny);
    }

    #[test]
    fn rejects_empty_targets() {
        let policies = [SerdePolicy::new(
            " ",
            SerdePolicyKind::StrictIngress,
            UnknownFieldPolicy::Deny,
        )];

        let err = validate_serde_policies(&policies).expect_err("empty target should fail validation");

        assert_eq!(err, SerdePolicyError::EmptyTarget { index: 0 });
    }

    #[test]
    fn rejects_strict_ingress_without_deny() {
        let policies = [SerdePolicy::new(
            "BucketPolicy",
            SerdePolicyKind::StrictIngress,
            UnknownFieldPolicy::Warn,
        )];

        let err = validate_serde_policies(&policies).expect_err("strict ingress should require deny");

        assert_eq!(err, SerdePolicyError::StrictIngressMustDeny { target: "BucketPolicy" });
    }

    #[test]
    fn rejects_compat_policy_with_deny() {
        let policies = [SerdePolicy::new(
            "LegacyConfig",
            SerdePolicyKind::PersistentLegacy,
            UnknownFieldPolicy::Deny,
        )];

        let err = validate_serde_policies(&policies).expect_err("compat policy should not deny unknown fields");

        assert_eq!(err, SerdePolicyError::CompatMustNotDeny { target: "LegacyConfig" });
    }

    #[test]
    fn rejects_duplicate_targets() {
        let policies = [
            SerdePolicy::new("BucketPolicy", SerdePolicyKind::StrictIngress, UnknownFieldPolicy::Deny),
            SerdePolicy::new("BucketPolicy", SerdePolicyKind::TolerantCompat, UnknownFieldPolicy::Warn),
        ];

        let err = validate_serde_policies(&policies).expect_err("duplicate target should fail validation");

        assert_eq!(err, SerdePolicyError::DuplicateTarget { target: "BucketPolicy" });
    }
}
