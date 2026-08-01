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

//! What still points at a KMS key, as far as the server can prove it.
//!
//! # This report never says a key is unused
//!
//! There is deliberately no `in_use`, no `unreferenced`, and no
//! `safe_to_delete`: the report states which sources were consulted
//! ([`ReferenceCoverage`]), how exhaustively they could be read
//! ([`ReferenceCompleteness`]), and what was found. An empty
//! [`KeyImpactReport::references`] therefore means "nothing was found in the
//! scanned sources", never "nothing references this key" — object envelopes
//! written under a key are not, and cannot cheaply be, enumerated here.
//! Collapsing that distinction into a boolean would hand callers a green
//! checkmark backed by an unscanned half of the problem, so the distinction
//! lives in the type rather than in prose a UI can skip.
//!
//! # It may only ever add a reason to refuse
//!
//! A report is an input to refusing destruction, never to permitting it.
//! [`KeyImpactReport::blocks_destruction`] is phrased so its `false` case
//! carries no authority: it means this report found no reason to refuse, not
//! that any other gate has been satisfied. The deletion worker's own
//! [`crate::DeletionReferenceChecker`] stays the gate that decides whether
//! expired material is destroyed.

use serde::{Deserialize, Serialize};

/// A place where references to a key can live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReferenceScope {
    /// A bucket's default server-side encryption configuration.
    BucketDefaultEncryption,
    /// The KMS service's configured default key.
    ServiceDefaultKey,
    /// Data-key envelopes stored on object versions.
    ObjectEnvelopes,
    /// Session envelopes of multipart uploads that have not completed yet.
    InProgressMultipartUploads,
}

/// Why an entry appears in [`KeyImpactReport::references`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KeyReferenceKind {
    /// A bucket's default encryption configuration names the key.
    BucketDefaultEncryption,
    /// The key is the KMS service's configured default key.
    ServiceDefaultKey,
    /// A whole source in [`ReferenceCoverage::scanned`] could not be
    /// enumerated. Reported as a reference, not as an absence: a source that
    /// cannot be read may hold references, and destroying material on the
    /// strength of an unanswered question is the one outcome that cannot be
    /// undone.
    UnreadableSource,
    /// One resource inside an otherwise readable source could not be
    /// inspected. Reported for the same reason as [`Self::UnreadableSource`].
    UnreadableResource,
}

/// One thing that points at a key, or one place that could not be checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyReference {
    /// Machine-readable category.
    pub kind: KeyReferenceKind,
    /// Identifier of the referencing resource: a bucket name for bucket
    /// configuration, the key id for the service default key, the affected
    /// source or resource for the unreadable kinds. Never key material.
    pub id: String,
    /// Human-readable detail. Identifiers only; never secrets or material.
    pub detail: String,
}

/// Which sources a report consulted, and which it did not look at at all.
///
/// `not_scanned` is mandatory rather than implied: a caller reading an empty
/// reference list has to be able to see, from the report alone, what was left
/// out of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceCoverage {
    /// Sources this report enumerated.
    pub scanned: Vec<ReferenceScope>,
    /// Sources this report does not cover at all.
    pub not_scanned: Vec<ReferenceScope>,
}

/// How far a report's reference list can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReferenceCompleteness {
    /// Every reference within [`ReferenceCoverage::scanned`] was enumerated.
    /// Reserved for configuration-layer facts, which are finite, cheap to
    /// read, and therefore exhaustively decidable.
    Exact,
    /// The list holds what a snapshot happened to observe within the scanned
    /// scopes. Absence of a reference is not evidence that none exists.
    ObservedOnly,
    /// At least one scanned source could not be read, so the list is not a
    /// statement about the key at all.
    Unavailable,
}

/// What the server can currently say about who points at a key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyImpactReport {
    /// Key the report is about.
    pub key_id: String,
    /// How far [`Self::references`] can be trusted.
    pub completeness: ReferenceCompleteness,
    /// Which sources were consulted and which were not.
    pub coverage: ReferenceCoverage,
    /// References found, plus one entry per source that could not be read.
    pub references: Vec<KeyReference>,
}

impl KeyImpactReport {
    /// An empty report over the configuration layer: bucket default encryption
    /// settings and the service default key.
    ///
    /// Both are finite and cheap to enumerate, so a report that reads them all
    /// stays [`ReferenceCompleteness::Exact`]; object-level scopes are
    /// declared as not scanned and stay that way.
    pub fn configuration_layer(key_id: impl Into<String>) -> Self {
        Self {
            key_id: key_id.into(),
            completeness: ReferenceCompleteness::Exact,
            coverage: ReferenceCoverage {
                scanned: vec![ReferenceScope::BucketDefaultEncryption, ReferenceScope::ServiceDefaultKey],
                not_scanned: vec![ReferenceScope::ObjectEnvelopes, ReferenceScope::InProgressMultipartUploads],
            },
            references: Vec::new(),
        }
    }

    /// Record one reference.
    ///
    /// An unreadable source or resource downgrades the report to
    /// [`ReferenceCompleteness::Unavailable`] here rather than at the call
    /// site, so no producer can report a partially read source as `Exact`.
    pub fn push_reference(&mut self, reference: KeyReference) {
        if matches!(
            reference.kind,
            KeyReferenceKind::UnreadableSource | KeyReferenceKind::UnreadableResource
        ) {
            self.completeness = ReferenceCompleteness::Unavailable;
        }
        self.references.push(reference);
    }

    /// Whether this report on its own is reason to refuse destroying the key's
    /// material right now.
    ///
    /// The two answers are not symmetric. `true` is a decision: something
    /// points at the key, or a source that might could not be read. `false`
    /// only means this report contributes no objection — it is never a
    /// clearance, and must not be used to skip, shorten, or satisfy any other
    /// check on the deletion path.
    pub fn blocks_destruction(&self) -> bool {
        // The completeness test is redundant while every producer records an
        // unreadable source as a reference, and stays here so that a future
        // producer which forgets to cannot turn an unanswered question into a
        // silent clearance.
        !self.references.is_empty() || matches!(self.completeness, ReferenceCompleteness::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_reference(bucket: &str) -> KeyReference {
        KeyReference {
            kind: KeyReferenceKind::BucketDefaultEncryption,
            id: bucket.to_string(),
            detail: format!("bucket {bucket} encrypts new objects with this key by default"),
        }
    }

    #[test]
    fn a_fresh_configuration_report_declares_the_object_layer_unscanned() {
        let report = KeyImpactReport::configuration_layer("kms-key-1");

        assert_eq!(report.completeness, ReferenceCompleteness::Exact);
        assert!(report.references.is_empty());
        assert_eq!(
            report.coverage.not_scanned,
            vec![ReferenceScope::ObjectEnvelopes, ReferenceScope::InProgressMultipartUploads],
            "an empty reference list is only readable next to what was left unscanned"
        );
        assert!(!report.blocks_destruction());
    }

    #[test]
    fn any_reference_blocks_destruction() {
        let mut report = KeyImpactReport::configuration_layer("kms-key-1");
        report.push_reference(bucket_reference("sse-bucket"));

        assert!(report.blocks_destruction());
        assert_eq!(
            report.completeness,
            ReferenceCompleteness::Exact,
            "a fully readable configuration layer stays exact even when it holds references"
        );
    }

    #[test]
    fn an_unreadable_source_is_never_reported_as_an_absence() {
        for kind in [KeyReferenceKind::UnreadableSource, KeyReferenceKind::UnreadableResource] {
            let mut report = KeyImpactReport::configuration_layer("kms-key-1");
            report.push_reference(KeyReference {
                kind,
                id: "bucket-default-encryption".to_string(),
                detail: "configuration could not be read".to_string(),
            });

            assert_eq!(
                report.completeness,
                ReferenceCompleteness::Unavailable,
                "{kind:?} must downgrade completeness"
            );
            assert!(report.blocks_destruction(), "{kind:?} must block destruction");
        }
    }

    #[test]
    fn unavailable_completeness_blocks_even_without_references() {
        // Guards the fail-closed fallback for a producer that marks a report
        // unavailable without recording why.
        let report = KeyImpactReport {
            completeness: ReferenceCompleteness::Unavailable,
            ..KeyImpactReport::configuration_layer("kms-key-1")
        };

        assert!(report.references.is_empty());
        assert!(report.blocks_destruction());
    }

    #[test]
    fn report_round_trips_through_json() {
        let mut report = KeyImpactReport::configuration_layer("kms-key-1");
        report.push_reference(bucket_reference("sse-bucket"));
        report.push_reference(KeyReference {
            kind: KeyReferenceKind::ServiceDefaultKey,
            id: "kms-key-1".to_string(),
            detail: "configured as the KMS service default key".to_string(),
        });

        let json = serde_json::to_string(&report).expect("serialization should succeed");
        let decoded: KeyImpactReport = serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(decoded, report);
    }

    /// The wire shape is the contract callers build UIs on: a boolean that
    /// reads as "this key is unused" must never appear in it, however the
    /// report is populated.
    #[test]
    fn the_wire_shape_asserts_nothing_about_absence_of_use() {
        let mut referenced = KeyImpactReport::configuration_layer("kms-key-1");
        referenced.push_reference(bucket_reference("sse-bucket"));

        for report in [KeyImpactReport::configuration_layer("kms-key-1"), referenced] {
            let json = serde_json::to_value(&report).expect("serialization should succeed");
            let mut fields: Vec<&str> = json.as_object().expect("report is a JSON object").keys().map(String::as_str).collect();
            fields.sort_unstable();

            assert_eq!(fields, vec!["completeness", "coverage", "key_id", "references"]);
            for forbidden in ["in_use", "unused", "unreferenced", "safe_to_delete", "deletable"] {
                assert!(!json.to_string().contains(forbidden), "report must not carry a `{forbidden}` claim");
            }
        }
    }
}
