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

use crate::filemeta::{ReplicationAction, ReplicationType};
use crate::http::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_TAGGING,
    AMZ_WEBSITE_REDIRECT_LOCATION, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, EXPIRES,
    has_prefix_fold, trim_etag,
};
use crate::tagging::ReplicationTagFilter;
use std::collections::HashMap;
use time::OffsetDateTime;

const AMZ_META_PREFIX: &str = "X-Amz-Meta-";
const CONTENT_ENCODING_LOWER: &str = "content-encoding";
const REPLICATION_METADATA_COMPARE_KEYS: [&str; 9] = [
    EXPIRES,
    CACHE_CONTROL,
    CONTENT_LANGUAGE,
    CONTENT_DISPOSITION,
    AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    AMZ_OBJECT_LOCK_LEGAL_HOLD,
    AMZ_WEBSITE_REDIRECT_LOCATION,
    AMZ_META_PREFIX,
];

#[derive(Debug, Clone)]
pub struct ReplicationSourceObject<'a> {
    pub mod_time: Option<OffsetDateTime>,
    pub version_id: Option<String>,
    pub etag: Option<&'a str>,
    pub actual_size: i64,
    pub delete_marker: bool,
    pub content_type: Option<&'a str>,
    pub content_encoding: Option<&'a str>,
    pub user_tags: &'a str,
    pub user_defined: &'a HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ReplicationTargetObject<'a> {
    pub last_modified: Option<OffsetDateTime>,
    pub version_id: Option<&'a str>,
    pub etag: Option<&'a str>,
    pub content_length: i64,
    pub delete_marker: bool,
    pub content_type: Option<&'a str>,
    pub metadata: Option<&'a HashMap<String, String>>,
    pub tag_count: i32,
}

pub fn content_matches_by_etag(source: &ReplicationSourceObject<'_>, target: &ReplicationTargetObject<'_>) -> bool {
    replication_etags_match(source.etag, target.etag)
}

pub fn replication_etags_match(source: Option<&str>, target: Option<&str>) -> bool {
    let source_etag = source.map(trim_etag);
    let target_etag = target.map(trim_etag);
    source_etag.is_some() && source_etag == target_etag
}

pub fn target_is_newer_than_source_null_version(
    source: &ReplicationSourceObject<'_>,
    target: &ReplicationTargetObject<'_>,
) -> bool {
    target
        .last_modified
        .is_some_and(|target_mod_time| target_mod_time > source.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH))
        && source.version_id.is_none()
}

pub fn replication_action_for_target(
    source: &ReplicationSourceObject<'_>,
    target: &ReplicationTargetObject<'_>,
    op_type: ReplicationType,
) -> ReplicationAction {
    if op_type == ReplicationType::ExistingObject && target_is_newer_than_source_null_version(source, target) {
        return ReplicationAction::None;
    }

    if source.etag.map(trim_etag) != target.etag.map(trim_etag)
        || source.version_id.as_deref() != target.version_id
        || source.actual_size != target.content_length
        || source.delete_marker != target.delete_marker
        || source.mod_time != target.last_modified
    {
        return ReplicationAction::All;
    }

    if source.content_type != target.content_type {
        return ReplicationAction::Metadata;
    }

    if content_encoding_differs(source, target) {
        return ReplicationAction::Metadata;
    }

    if tag_metadata_differs(source, target) {
        return ReplicationAction::Metadata;
    }

    if comparable_metadata(Some(source.user_defined)) != comparable_metadata(target.metadata) {
        return ReplicationAction::Metadata;
    }

    ReplicationAction::None
}

fn content_encoding_differs(source: &ReplicationSourceObject<'_>, target: &ReplicationTargetObject<'_>) -> bool {
    if let Some(content_encoding) = source.content_encoding {
        return target
            .metadata
            .and_then(|metadata| {
                metadata
                    .get(CONTENT_ENCODING)
                    .or_else(|| metadata.get(CONTENT_ENCODING_LOWER))
            })
            .is_none_or(|enc| enc != content_encoding);
    }
    false
}

fn tag_metadata_differs(source: &ReplicationSourceObject<'_>, target: &ReplicationTargetObject<'_>) -> bool {
    let source_tags = ReplicationTagFilter::decode_tags_to_map(source.user_tags);
    let target_tagging = target
        .metadata
        .and_then(|metadata| metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str))
        .unwrap_or_default();
    let target_tags = ReplicationTagFilter::decode_tags_to_map(target_tagging);
    let source_tag_count = i32::try_from(source_tags.len()).unwrap_or(i32::MAX);

    (target.tag_count > 0 && source_tags != target_tags) || target.tag_count != source_tag_count
}

fn comparable_metadata(metadata: Option<&HashMap<String, String>>) -> HashMap<String, String> {
    let mut comparable = HashMap::new();
    for (key, value) in metadata.into_iter().flatten() {
        if REPLICATION_METADATA_COMPARE_KEYS
            .iter()
            .any(|prefix| has_prefix_fold(key, prefix))
        {
            comparable.insert(key.to_lowercase(), value.clone());
        }
    }
    comparable
}

/// Runtime half of the P1-19 version-identity contract (the explicit probe
/// lives in replication-check's VersionFidelity phase): every replication PUT
/// response reveals whether the target adopted the source version id. A
/// target minting its own ids silently breaks version-addressed deletes and
/// heal, so surface it — once per target — instead of letting the divergence
/// accumulate unseen.
/// Pure drift judgment: the contract only applies when the source addressed a
/// real (non-nil) version uuid, and drift means the target answered with
/// anything else — including nothing at all.
pub fn version_identity_drifted(source_version_id: &str, assigned_version_id: Option<&str>) -> bool {
    if source_version_id.is_empty() {
        return false;
    }
    // A nil source uuid travels as the literal "null" (unversioned-source
    // semantics); no identity contract applies to it.
    if uuid::Uuid::parse_str(source_version_id)
        .map(|uuid| uuid.is_nil())
        .unwrap_or(true)
    {
        return false;
    }
    assigned_version_id != Some(source_version_id)
}

const REPLICATION_TARGET_OFFLINE_ERROR_MARKERS: &[&str] = &[
    "dispatch failure",
    "timeouterror",
    "timed out",
    "connection refused",
    "connection reset",
    "connection closed",
    "connection aborted",
    "broken pipe",
    "dns error",
    "failed to lookup address",
    "name or service not known",
    "deadline has elapsed",
    "tcp connect error",
];

/// True when a target operation error reads as a network/transport failure —
/// the only class of error that should mark a replication target offline.
pub fn is_replication_target_offline_error(err: &(impl std::fmt::Display + ?Sized)) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    REPLICATION_TARGET_OFFLINE_ERROR_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
}

/// Whether a replication target preserves the SSE-C passthrough transport
/// headers (`X-Rustfs-Replication-*`) end to end.
///
/// A target that silently drops those headers (MinIO, generic S3) stores the
/// forwarded ciphertext without its decryption material — an unreadable
/// replica that used to report COMPLETED. The replication worker audits the
/// first passthrough PUT per target (HEAD-back for SSE-C evidence) and caches
/// the verdict; a fresh `Unsupported` fails SSE-C replication closed before
/// any PUT is sent. The verdict cache (per-ARN map, lifecycle, and TTL) is
/// owned by the runtime's bucket target system; this crate owns only the
/// verdict vocabulary and the gate policy below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SsecPassthroughCapability {
    #[default]
    Unknown,
    Supported,
    Unsupported,
}

/// Fail-closed decision for an SSE-C passthrough replication attempt, derived
/// from the target's cached [`SsecPassthroughCapability`]. Pure so the policy
/// can migrate with the worker (M2) without dragging the cache along; the
/// caller computes `expired` from the cache record's age (see the runtime's
/// `SSEC_PASSTHROUGH_CAPABILITY_TTL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsecPassthroughGate {
    /// Not an SSE-C object, or the target has a fresh proof that it preserves
    /// the passthrough transport headers: replicate without a HEAD-back audit.
    Proceed,
    /// No usable verdict — first SSE-C attempt since the target was (re)built,
    /// or the recorded verdict (in either direction) aged out: PUT, then HEAD
    /// the replica back and require SSE-C evidence before reporting COMPLETED.
    ProceedWithAudit,
    /// The target was recently proven to drop the passthrough headers: do not
    /// send the PUT, report FAILED (the object stays on the normal MRF retry
    /// channel and re-audits once the verdict expires).
    FailClosed,
}

pub fn ssec_passthrough_gate(ssec: bool, capability: SsecPassthroughCapability, expired: bool) -> SsecPassthroughGate {
    if !ssec {
        return SsecPassthroughGate::Proceed;
    }
    // An expired verdict — Supported or Unsupported — must be re-earned: a
    // stale Unsupported would otherwise stick forever after a target upgrade,
    // and a stale Supported would fail open after a backend swap behind the
    // same endpoint.
    if expired {
        return SsecPassthroughGate::ProceedWithAudit;
    }
    match capability {
        SsecPassthroughCapability::Supported => SsecPassthroughGate::Proceed,
        SsecPassthroughCapability::Unknown => SsecPassthroughGate::ProceedWithAudit,
        SsecPassthroughCapability::Unsupported => SsecPassthroughGate::FailClosed,
    }
}

/// True when a replication-check HEAD of the replica proves the SSE-C
/// material survived passthrough: a RustFS target restores the transport
/// headers into the stored SSE-C keys and its HEAD echoes
/// `x-amz-server-side-encryption-customer-algorithm` (the replication-check
/// exemption skips key validation but not the metadata echo). A target that
/// dropped the headers stored a plain object and echoes nothing. The caller
/// extracts the echoed customer-algorithm value from its HEAD response type.
pub fn ssec_passthrough_evidence_present(sse_customer_algorithm: Option<&str>) -> bool {
    sse_customer_algorithm.is_some_and(|algo| !algo.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{
        ReplicationSourceObject, ReplicationTargetObject, SsecPassthroughCapability, SsecPassthroughGate,
        content_matches_by_etag, is_replication_target_offline_error, replication_action_for_target, replication_etags_match,
        ssec_passthrough_evidence_present, ssec_passthrough_gate, target_is_newer_than_source_null_version,
        version_identity_drifted,
    };
    use crate::filemeta::{ReplicationAction, ReplicationType};
    use crate::http::AMZ_OBJECT_LOCK_MODE;
    use std::collections::HashMap;
    use time::{Duration, OffsetDateTime};

    fn source_object(user_defined: &HashMap<String, String>) -> ReplicationSourceObject<'_> {
        ReplicationSourceObject {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            version_id: Some("source-version".to_string()),
            etag: Some("\"abc\""),
            actual_size: 10,
            delete_marker: false,
            content_type: Some("text/plain"),
            content_encoding: None,
            user_tags: "a=1",
            user_defined,
        }
    }

    fn target_object(metadata: &HashMap<String, String>) -> ReplicationTargetObject<'_> {
        ReplicationTargetObject {
            last_modified: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            version_id: Some("source-version"),
            etag: Some("abc"),
            content_length: 10,
            delete_marker: false,
            content_type: Some("text/plain"),
            metadata: Some(metadata),
            tag_count: 1,
        }
    }

    #[test]
    fn content_matches_by_etag_ignores_version_ids() {
        let source_metadata = HashMap::new();
        let target_metadata = HashMap::new();
        let source = ReplicationSourceObject {
            version_id: Some("source-version".to_string()),
            etag: Some("\"abc\""),
            user_defined: &source_metadata,
            ..source_object(&source_metadata)
        };
        let target = ReplicationTargetObject {
            version_id: Some("different-version"),
            etag: Some("abc"),
            metadata: Some(&target_metadata),
            ..target_object(&target_metadata)
        };

        assert!(content_matches_by_etag(&source, &target));
        assert!(replication_etags_match(source.etag, target.etag));
    }

    #[test]
    fn target_newer_null_version_skips_existing_object_replication() {
        let source_metadata = HashMap::new();
        let target_metadata = HashMap::new();
        let source = ReplicationSourceObject {
            version_id: None,
            ..source_object(&source_metadata)
        };
        let target = ReplicationTargetObject {
            last_modified: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(20)),
            ..target_object(&target_metadata)
        };

        assert!(target_is_newer_than_source_null_version(&source, &target));
        assert_eq!(
            replication_action_for_target(&source, &target, ReplicationType::ExistingObject),
            ReplicationAction::None
        );
    }

    #[test]
    fn replication_action_detects_content_and_metadata_differences() {
        let mut source_metadata = HashMap::new();
        source_metadata.insert("Cache-Control".to_string(), "max-age=1".to_string());
        let mut target_metadata = HashMap::new();
        target_metadata.insert("X-Amz-Tagging".to_string(), "a=1".to_string());
        target_metadata.insert("Cache-Control".to_string(), "max-age=1".to_string());

        let source = source_object(&source_metadata);
        let target = target_object(&target_metadata);
        assert_eq!(
            replication_action_for_target(&source, &target, ReplicationType::ExistingObject),
            ReplicationAction::None
        );

        let changed_content = ReplicationTargetObject {
            content_length: 11,
            ..target_object(&target_metadata)
        };
        assert_eq!(
            replication_action_for_target(&source, &changed_content, ReplicationType::ExistingObject),
            ReplicationAction::All
        );

        let mut changed_target_metadata = target_metadata.clone();
        changed_target_metadata.insert("Cache-Control".to_string(), "max-age=2".to_string());
        let changed_metadata = target_object(&changed_target_metadata);
        assert_eq!(
            replication_action_for_target(&source, &changed_metadata, ReplicationType::ExistingObject),
            ReplicationAction::Metadata
        );
    }

    /// P1-19 runtime spot-check exemption matrix: drift only applies when the
    /// source addressed a real version uuid.
    #[test]
    fn test_version_identity_drift_judgment() {
        let source = "6fa459ea-ee8a-3ca4-894e-db77e160355e";
        for (sent, got, expected) in [
            (source, Some(source), false),
            (source, Some("0e304ce5-33e9-4b8a-9b12-9e40a53e6ded"), true),
            (source, None, true),
            ("", None, false),
            ("null", Some("anything"), false),
            ("00000000-0000-0000-0000-000000000000", Some("anything"), false),
        ] {
            assert_eq!(
                version_identity_drifted(sent, got),
                expected,
                "sent {sent:?} got {got:?} must judge drift = {expected}"
            );
        }
    }

    #[test]
    fn replication_target_offline_error_classifier_is_network_scoped() {
        assert!(is_replication_target_offline_error("put_object dispatch failure: connector error"));
        assert!(is_replication_target_offline_error("request TimeoutError after retry"));
        assert!(is_replication_target_offline_error("tcp connect error: connection refused"));
        assert!(!is_replication_target_offline_error("put_object failed: AccessDenied: denied"));
        assert!(!is_replication_target_offline_error("put_object failed: NoSuchBucket"));
    }

    /// N2 fail-closed policy: SSE-C replication may only proceed silently
    /// against a target with a FRESH proof that it preserves the passthrough
    /// transport headers. Unknown targets must be audited; freshly-flagged
    /// dropping targets must never receive the PUT; an expired verdict in
    /// EITHER direction must be re-earned through the audit — a sticky
    /// Unsupported would outlive a target upgrade, and a sticky Supported
    /// would fail open after a backend swap behind the same endpoint.
    #[test]
    fn ssec_passthrough_gate_is_fail_closed_and_ttl_bounded() {
        for capability in [
            SsecPassthroughCapability::Unknown,
            SsecPassthroughCapability::Supported,
            SsecPassthroughCapability::Unsupported,
        ] {
            for expired in [false, true] {
                assert_eq!(
                    ssec_passthrough_gate(false, capability, expired),
                    SsecPassthroughGate::Proceed,
                    "non-SSE-C objects must never be gated on the passthrough capability"
                );
            }
        }
        assert_eq!(
            ssec_passthrough_gate(true, SsecPassthroughCapability::Supported, false),
            SsecPassthroughGate::Proceed
        );
        assert_eq!(
            ssec_passthrough_gate(true, SsecPassthroughCapability::Unknown, false),
            SsecPassthroughGate::ProceedWithAudit
        );
        assert_eq!(
            ssec_passthrough_gate(true, SsecPassthroughCapability::Unsupported, false),
            SsecPassthroughGate::FailClosed
        );
        // Expiry flips both directions back to the audit.
        assert_eq!(
            ssec_passthrough_gate(true, SsecPassthroughCapability::Unsupported, true),
            SsecPassthroughGate::ProceedWithAudit,
            "an expired Unsupported verdict must allow a re-audit (upgraded target recovers without operator action)"
        );
        assert_eq!(
            ssec_passthrough_gate(true, SsecPassthroughCapability::Supported, true),
            SsecPassthroughGate::ProceedWithAudit,
            "an expired Supported verdict must be re-proven (backend swap behind the same endpoint must not fail open)"
        );
    }

    #[test]
    fn ssec_passthrough_evidence_requires_customer_algorithm_echo() {
        assert!(ssec_passthrough_evidence_present(Some("AES256")));
        assert!(
            !ssec_passthrough_evidence_present(Some("")),
            "an empty echo is not evidence of preserved SSE-C material"
        );
        assert!(
            !ssec_passthrough_evidence_present(None),
            "a plain HEAD response must classify the target as having dropped the material"
        );
    }

    #[test]
    fn replication_action_detects_tags_and_object_lock_metadata_differences() {
        let mut source_metadata = HashMap::new();
        source_metadata.insert(AMZ_OBJECT_LOCK_MODE.to_string(), "GOVERNANCE".to_string());
        let source = ReplicationSourceObject {
            user_tags: "a=1&b=2",
            user_defined: &source_metadata,
            ..source_object(&source_metadata)
        };

        let target_metadata = HashMap::new();
        let target = ReplicationTargetObject {
            tag_count: 1,
            metadata: Some(&target_metadata),
            ..target_object(&target_metadata)
        };

        assert_eq!(
            replication_action_for_target(&source, &target, ReplicationType::Metadata),
            ReplicationAction::Metadata
        );
    }
}
