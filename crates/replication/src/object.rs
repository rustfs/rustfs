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

fn is_plain_single_part_md5(etag: &str) -> bool {
    etag.len() == 32 && etag.bytes().all(|b| b.is_ascii_hexdigit())
}

/// How a replication PutObject that carries Object Lock parameters satisfies
/// the target-side rule that such a request must also carry `Content-MD5` or
/// an `x-amz-checksum-*` header (AWS S3, MinIO and most compatible stores
/// enforce it; rustfs#7082).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLockIntegrity {
    /// No Object Lock parameters, or an integrity header is already present.
    NotRequired,
    /// Send `Content-MD5` computed from this hex MD5: the source ETag is the
    /// MD5 of exactly the bytes going on the wire, so no body pass and no
    /// change of payload framing is needed.
    ContentMd5Hex(String),
    /// The source ETag is not the MD5 of the wire bytes (multipart layout,
    /// or an encrypted object whose ETag does not describe the plaintext);
    /// let the SDK compute a checksum instead.
    SdkChecksum,
}

/// Decide the integrity header for a locked replication PUT.
///
/// `plaintext_end_to_end` is false when the request announces any server-side
/// encryption or carries SSE-C ciphertext passthrough headers: the source
/// ETag then does not describe the bytes on the wire and must not be turned
/// into a `Content-MD5` the target would reject with `BadDigest`.
pub fn object_lock_put_integrity(
    lock_params: bool,
    has_integrity_header: bool,
    plaintext_end_to_end: bool,
    source_etag: Option<&str>,
) -> ObjectLockIntegrity {
    if !lock_params || has_integrity_header {
        return ObjectLockIntegrity::NotRequired;
    }
    match source_etag.map(trim_etag) {
        Some(etag) if plaintext_end_to_end && is_plain_single_part_md5(&etag) => {
            ObjectLockIntegrity::ContentMd5Hex(etag.to_ascii_lowercase())
        }
        _ => ObjectLockIntegrity::SdkChecksum,
    }
}

/// Whether the ETag the target returned for a single-part replica proves the
/// stored bytes differ from what the source sent — e.g. a target that does not
/// decode `aws-chunked` framing stores the frames verbatim and returns their
/// ETag. Only a plain single-part MD5 ETag on both sides is decidable; a
/// multipart or opaque (encrypted) ETag, or a withheld replica ETag, returns
/// `false` because no corruption can be concluded from it.
pub fn single_part_replica_etag_mismatch(source_etag: Option<&str>, replica_etag: Option<&str>) -> bool {
    let Some(source) = source_etag.map(trim_etag) else {
        return false;
    };
    if !is_plain_single_part_md5(&source) {
        return false;
    }
    let Some(replica) = replica_etag.map(trim_etag) else {
        return false;
    };
    if !is_plain_single_part_md5(&replica) {
        return false;
    }
    !source.eq_ignore_ascii_case(&replica)
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
    version_identity_capability_from_put(source_version_id, assigned_version_id) == Some(VersionIdentityCapability::MintsOwn)
}

/// Whether a replication target adopts the source version id it is handed on
/// PutObject / CompleteMultipartUpload, or mints its own.
///
/// A target that mints its own ids (AWS S3, Wasabi, Impossible Cloud) still
/// stores the bytes, but every later version-addressed request from the
/// source names an id the target never had. Its HEAD then answers 404 —
/// indistinguishable from a replica that is really missing — so a heal, MRF
/// retry or existing-object resync re-drive would PUT the object again and
/// mint yet another target version (rustfs/backlog#2340). The replication
/// worker learns the verdict from each PUT response (and replication-check's
/// VersionFidelity phase) and, once `MintsOwn` is known, locates a replica by
/// exact key and ETag before concluding that it is missing. The verdict cache
/// is owned by the runtime's bucket target system; this crate owns only the
/// vocabulary and the judgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VersionIdentityCapability {
    #[default]
    Unknown,
    Adopts,
    MintsOwn,
}

impl VersionIdentityCapability {
    /// True when a 404 from a version-addressed HEAD on this target cannot be
    /// read as "replica missing": the source-side id was never the target's.
    pub fn version_addressing_unreliable(self) -> bool {
        self == VersionIdentityCapability::MintsOwn
    }
}

/// Judge the identity contract from one replication write: `None` when no
/// contract applies (the source addressed no real version — an empty or nil
/// uuid travels as the literal "null", unversioned-source semantics),
/// otherwise whether the target echoed the source id or answered with
/// anything else — including nothing at all.
pub fn version_identity_capability_from_put(
    source_version_id: &str,
    assigned_version_id: Option<&str>,
) -> Option<VersionIdentityCapability> {
    if source_version_id.is_empty() {
        return None;
    }
    if uuid::Uuid::parse_str(source_version_id)
        .map(|uuid| uuid.is_nil())
        .unwrap_or(true)
    {
        return None;
    }
    Some(if assigned_version_id == Some(source_version_id) {
        VersionIdentityCapability::Adopts
    } else {
        VersionIdentityCapability::MintsOwn
    })
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
    use super::{ObjectLockIntegrity, object_lock_put_integrity};

    const SOURCE_MD5: &str = "9a0364b9e99bb480dd25e1f0284c8555";
    const FRAMED_MD5: &str = "0f343b0931126a20f133d67c2b018a3b";

    #[test]
    fn single_part_replica_mismatch_is_only_decided_on_plain_md5_pairs() {
        // The #6853 shape: the target stored aws-chunked frames verbatim and
        // returned the framed bytes' ETag.
        assert!(single_part_replica_etag_mismatch(Some(SOURCE_MD5), Some(FRAMED_MD5)));
        assert!(single_part_replica_etag_mismatch(
            Some(&format!("\"{SOURCE_MD5}\"")),
            Some(&format!("\"{FRAMED_MD5}\""))
        ));

        // A faithful replica, quoted or not, passes; hex case must not matter
        // (a target may return the same MD5 uppercased).
        assert!(!single_part_replica_etag_mismatch(Some(SOURCE_MD5), Some(SOURCE_MD5)));
        assert!(!single_part_replica_etag_mismatch(Some(&format!("\"{SOURCE_MD5}\"")), Some(SOURCE_MD5)));
        assert!(!single_part_replica_etag_mismatch(
            Some(SOURCE_MD5),
            Some(&SOURCE_MD5.to_ascii_uppercase())
        ));

        // Not decidable: multipart source, opaque replica ETag, or either side
        // missing must never be reported as corruption.
        assert!(!single_part_replica_etag_mismatch(Some(&format!("{SOURCE_MD5}-3")), Some(FRAMED_MD5)));
        assert!(!single_part_replica_etag_mismatch(Some(SOURCE_MD5), Some(&format!("{FRAMED_MD5}-3"))));
        assert!(!single_part_replica_etag_mismatch(Some(SOURCE_MD5), None));
        assert!(!single_part_replica_etag_mismatch(None, Some(FRAMED_MD5)));
    }

    use super::{
        ReplicationSourceObject, ReplicationTargetObject, SsecPassthroughCapability, SsecPassthroughGate,
        VersionIdentityCapability, content_matches_by_etag, is_replication_target_offline_error, replication_action_for_target,
        replication_etags_match, single_part_replica_etag_mismatch, ssec_passthrough_evidence_present, ssec_passthrough_gate,
        target_is_newer_than_source_null_version, version_identity_capability_from_put, version_identity_drifted,
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
    fn version_identity_capability_is_judged_only_for_real_source_versions() {
        let source = "8e4d2f4c-2d5c-4f1b-9d0a-9c8b7a6f5e4d";
        assert_eq!(
            version_identity_capability_from_put(source, Some(source)),
            Some(VersionIdentityCapability::Adopts)
        );
        // Wasabi / AWS shape: a minted id, or no id at all, both mean the
        // source-side id is not addressable on the target.
        assert_eq!(
            version_identity_capability_from_put(source, Some("001788697733811332140-fR6j6uXKV-")),
            Some(VersionIdentityCapability::MintsOwn)
        );
        assert_eq!(
            version_identity_capability_from_put(source, None),
            Some(VersionIdentityCapability::MintsOwn)
        );
        // No contract for an unversioned source write.
        assert_eq!(version_identity_capability_from_put("", Some("anything")), None);
        assert_eq!(version_identity_capability_from_put("00000000-0000-0000-0000-000000000000", None), None);
        assert_eq!(version_identity_capability_from_put("null", Some("null")), None);
        assert!(VersionIdentityCapability::MintsOwn.version_addressing_unreliable());
        assert!(!VersionIdentityCapability::Adopts.version_addressing_unreliable());
        assert!(!VersionIdentityCapability::Unknown.version_addressing_unreliable());
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

    #[test]
    fn locked_put_uses_the_plain_source_md5_as_content_md5() {
        assert_eq!(
            object_lock_put_integrity(true, false, true, Some("\"9A0364B9E99BB480DD25E1F0284C8555\"")),
            ObjectLockIntegrity::ContentMd5Hex("9a0364b9e99bb480dd25e1f0284c8555".to_string())
        );
    }

    #[test]
    fn locked_put_without_a_usable_etag_falls_back_to_the_sdk_checksum() {
        // Multipart layout: the ETag is not the MD5 of the body.
        assert_eq!(
            object_lock_put_integrity(true, false, true, Some("9a0364b9e99bb480dd25e1f0284c8555-2")),
            ObjectLockIntegrity::SdkChecksum
        );
        // Encrypted end to end: the ETag does not describe the wire bytes.
        assert_eq!(
            object_lock_put_integrity(true, false, false, Some("9a0364b9e99bb480dd25e1f0284c8555")),
            ObjectLockIntegrity::SdkChecksum
        );
        assert_eq!(object_lock_put_integrity(true, false, true, None), ObjectLockIntegrity::SdkChecksum);
        assert_eq!(object_lock_put_integrity(true, false, true, Some("")), ObjectLockIntegrity::SdkChecksum);
    }

    #[test]
    fn integrity_is_not_added_without_lock_params_or_when_already_present() {
        assert_eq!(
            object_lock_put_integrity(false, false, true, Some("9a0364b9e99bb480dd25e1f0284c8555")),
            ObjectLockIntegrity::NotRequired
        );
        assert_eq!(
            object_lock_put_integrity(true, true, true, Some("9a0364b9e99bb480dd25e1f0284c8555")),
            ObjectLockIntegrity::NotRequired
        );
    }
}
