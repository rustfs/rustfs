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

//! Canonical metadata keys persisted for encrypted objects and the replication
//! transport mapping that carries SSE-C material between sites.
//!
//! The stored-key constants are the single source of truth shared by the SSE
//! writer (`rustfs::storage::sse`), the replication boundary (`rustfs_ecstore`),
//! and log redaction (`rustfs_filemeta`). Keys listed in
//! [`SSEC_REPLICATION_TRANSPORT_HEADERS`] are renamed onto the wire for SSE-C
//! ciphertext passthrough; every other encryption key must be stripped from
//! outbound replication metadata via [`is_replication_stripped_encryption_key`].

// The lowercase stored forms, matching exactly what encryption_material_to_metadata
// persists. The read-path SSE-C check is case-sensitive, so restoring under any
// other casing would classify the replica as managed-SSE and reject SSE-C GETs.
use super::headers::{AMZ_ENCRYPTION_KMS, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER};
use std::collections::HashMap;

pub const INTERNAL_ENCRYPTION_KEY_ID_HEADER: &str = "x-rustfs-encryption-key-id";
pub const INTERNAL_ENCRYPTION_KEY_HEADER: &str = "x-rustfs-encryption-key";
pub const INTERNAL_ENCRYPTION_IV_HEADER: &str = "x-rustfs-encryption-iv";
/// Carries the AEAD algorithm the object was sealed with.
///
/// The S3 `x-amz-server-side-encryption` header records the *SSE mode*
/// (`AES256` / `aws:kms`), not the cipher, so it cannot round-trip
/// `ChaCha20Poly1305`. Without this header a ChaCha-sealed object comes back
/// from the projection claiming `aws:kms` and is then opened with the wrong
/// cipher.
pub const INTERNAL_ENCRYPTION_ALGORITHM_HEADER: &str = "x-rustfs-encryption-algorithm";
pub const INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER: &str = "x-rustfs-encryption-original-size";
pub const INTERNAL_ENCRYPTION_CONTEXT_HEADER: &str = "x-rustfs-encryption-context";
pub const INTERNAL_ENCRYPTION_TAG_HEADER: &str = "x-rustfs-encryption-tag";
pub const SSEC_ORIGINAL_SIZE_HEADER: &str = "x-amz-server-side-encryption-customer-original-size";
pub const MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER: &str = "X-Minio-Internal-Encrypted-Multipart";
pub const MINIO_INTERNAL_ENCRYPTION_IV_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Iv";
pub const MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm";
pub const MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Sealed-Key";
pub const MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key";
pub const MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key";
pub const MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id";
pub const MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key";
pub const MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Context";

/// Reserved RustFS-branded twin of the MinIO-internal SSE key family.
///
/// No RustFS writer emits these keys today — the SSE writer persists the
/// MinIO-branded `X-Minio-Internal-Server-Side-Encryption-*` keys verbatim for
/// interoperability — but redaction (`rustfs_filemeta`) and replication
/// stripping treat the family as sensitive so that a future or third-party
/// writer cannot leak sealed material through the reserved names.
pub const RUSTFS_INTERNAL_ENCRYPTION_PREFIX: &str = "x-rustfs-internal-server-side-encryption-";

pub const REPLICATION_SSEC_ALGORITHM_HEADER: &str = "X-Rustfs-Replication-Ssec-Algorithm";
pub const REPLICATION_SSEC_KEY_MD5_HEADER: &str = "X-Rustfs-Replication-Ssec-Key-Md5";
pub const REPLICATION_SSEC_ORIGINAL_SIZE_HEADER: &str = "X-Rustfs-Replication-Ssec-Original-Size";
pub const REPLICATION_ENCRYPTION_IV_HEADER: &str = "X-Rustfs-Replication-Encryption-Iv";
pub const REPLICATION_SSE_IV_HEADER: &str = "X-Rustfs-Replication-Server-Side-Encryption-Iv";
pub const REPLICATION_SSE_SEAL_ALGORITHM_HEADER: &str = "X-Rustfs-Replication-Server-Side-Encryption-Seal-Algorithm";
pub const REPLICATION_SSE_SEALED_KEY_HEADER: &str = "X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key";
pub const REPLICATION_ENCRYPTED_MULTIPART_HEADER: &str = "X-Rustfs-Replication-Encrypted-Multipart";

/// Stored SSE-C metadata keys and the wire names they replicate under.
///
/// Source keys must match what `encryption_material_to_metadata` persists; the
/// reconciliation test in `rustfs::storage::sse` pins that correspondence.
pub const SSEC_REPLICATION_TRANSPORT_HEADERS: &[(&str, &str)] = &[
    (SSEC_ALGORITHM_HEADER, REPLICATION_SSEC_ALGORITHM_HEADER),
    (SSEC_KEY_MD5_HEADER, REPLICATION_SSEC_KEY_MD5_HEADER),
    (SSEC_ORIGINAL_SIZE_HEADER, REPLICATION_SSEC_ORIGINAL_SIZE_HEADER),
    (INTERNAL_ENCRYPTION_IV_HEADER, REPLICATION_ENCRYPTION_IV_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_IV_HEADER, REPLICATION_SSE_IV_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, REPLICATION_SSE_SEAL_ALGORITHM_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, REPLICATION_SSE_SEALED_KEY_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER, REPLICATION_ENCRYPTED_MULTIPART_HEADER),
];

/// Retains only the SSE-C headers consumed by object readers and marks their
/// values sensitive so instrumented storage calls cannot expose key material.
pub fn project_ssec_transport_headers(headers: &http::HeaderMap) -> http::HeaderMap {
    let mut projected = http::HeaderMap::new();
    for name in [SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER] {
        if let Some(value) = headers.get(name) {
            let mut value = value.clone();
            value.set_sensitive(true);
            projected.insert(name, value);
        }
    }
    projected
}

/// Prefixes of replication SSE transport keys whose values carry encryption
/// material and must never reach logs. Consumed by `rustfs_filemeta` redaction.
pub const REPLICATION_SSE_TRANSPORT_PREFIXES: &[&str] = &[
    "x-rustfs-replication-server-side-encryption-",
    "x-rustfs-replication-encryption-",
    "x-rustfs-replication-ssec-",
];

/// Returns true when the request carries any SSE-C replication transport
/// header — the signal that an authorized replication PUT is a ciphertext
/// passthrough and the receiver must not re-encrypt or compress the body.
pub fn has_ssec_transport_headers(headers: &http::HeaderMap) -> bool {
    headers.keys().any(|name| {
        let name = name.as_str();
        REPLICATION_SSE_TRANSPORT_PREFIXES
            .iter()
            .any(|prefix| super::starts_with_ignore_ascii_case(name, prefix))
            || name.eq_ignore_ascii_case(REPLICATION_ENCRYPTED_MULTIPART_HEADER)
    })
}

/// Restores the stored SSE-C metadata keys from their replication transport
/// names. Returns None when the request carries no transport headers. When the
/// customer algorithm is present, the AES256 SSE marker is re-added so the
/// restored metadata matches the shape `encryption_material_to_metadata`
/// persists (SSE-C Direct writes both IV twins; each travels under its own
/// transport name, so the 1:1 reverse mapping restores the dual-key pair).
pub fn ssec_transport_to_stored_metadata(headers: &http::HeaderMap) -> Option<std::collections::HashMap<String, String>> {
    let mut restored = std::collections::HashMap::new();
    for (stored, transport) in SSEC_REPLICATION_TRANSPORT_HEADERS {
        if let Some(value) = headers.get(*transport).and_then(|value| value.to_str().ok()) {
            restored.insert((*stored).to_string(), value.to_string());
        }
    }
    if restored.is_empty() {
        return None;
    }
    if restored.contains_key(SSEC_ALGORITHM_HEADER) {
        restored.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
    }
    Some(restored)
}

/// Maps a stored SSE-C metadata key to its replication transport name.
pub fn ssec_replication_transport_header(stored_key: &str) -> Option<&'static str> {
    SSEC_REPLICATION_TRANSPORT_HEADERS
        .iter()
        .find(|(stored, _)| stored.eq_ignore_ascii_case(stored_key))
        .map(|(_, transport)| *transport)
}

/// Returns true for metadata keys that must never leave the source site as
/// plain replication metadata: encryption envelopes, SSE intent headers, and
/// SSE-C material. SSE-C passthrough re-adds its keys through the transport
/// mapping instead.
pub fn is_replication_stripped_encryption_key(key: &str) -> bool {
    // The x-rustfs-internal- SSE prefix is a reserved name family with no
    // writer today (see RUSTFS_INTERNAL_ENCRYPTION_PREFIX); cover it here so
    // this predicate is safe to use standalone, without an is_internal_key
    // backstop.
    super::is_encryption_metadata_key(key)
        || super::is_sse_header(key)
        || key.eq_ignore_ascii_case(SSEC_ORIGINAL_SIZE_HEADER)
        || super::starts_with_ignore_ascii_case(key, RUSTFS_INTERNAL_ENCRYPTION_PREFIX)
}

// ============================================================================
// Managed-SSE attribution (shared classifier)
// ============================================================================
//
// Single source of truth for classifying stored managed-SSE (SSE-S3 / SSE-KMS)
// object metadata. These live here — rather than in the `rustfs` binary
// crate's SSE module — so lower-layer consumers such as the scanner can
// attribute encrypted objects without growing a second copy of the
// normalization/classification logic (backlog#1643 PR-B0). The binary crate
// re-exports them from `rustfs::storage::sse`, and a source-scan test there
// pins that no second definition reappears.
//
// Every metadata lookup below is a case-SENSITIVE exact match on the stored
// `HashMap<String, String>` keys, mirroring the SSE read path. Do not
// "harmonize" these with the lowercase-normalizing helpers in
// `header_compat.rs`: the lowercase `x-amz-*` stored forms and the TitleCase
// MinIO-internal names are load-bearing exactly as written.

/// Type of encryption used
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SSEType {
    /// SSE-S3 (AES256)
    SseS3,
    /// SSE-KMS (aws:kms)
    SseKms,
    /// SSE-C (customer-provided key)
    SseC,
}

impl SSEType {
    /// Stable scheme name for audit consumers.
    pub fn audit_label(self) -> &'static str {
        match self {
            SSEType::SseS3 => "SSE-S3",
            SSEType::SseKms => "SSE-KMS",
            SSEType::SseC => "SSE-C",
        }
    }
}

/// Recodes a stored MinIO KMS context value — base64-wrapped JSON under
/// [`MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER`] — into the plain-JSON form
/// RustFS stores under [`INTERNAL_ENCRYPTION_CONTEXT_HEADER`].
///
/// Injected by callers because this crate deliberately carries no JSON codec.
/// Returning `None` skips the context mapping, matching the historical
/// silent-skip on a value that fails to decode.
pub type KmsContextRecoder = fn(&str) -> Option<String>;

/// True when the stored metadata carries a managed-SSE (SSE-S3 / SSE-KMS)
/// encryption envelope, under either the RustFS-branded or the MinIO-branded
/// internal keys.
pub fn contains_managed_encryption_metadata(metadata: &HashMap<String, String>) -> bool {
    metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER)
        || metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
        || metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
        || metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)
        || metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
}

/// Maps the MinIO-branded internal SSE keys onto the RustFS-branded stored
/// keys (the dual internal metadata keys invariant). RustFS-branded keys
/// already present always win; every source lookup is a case-sensitive exact
/// match on the specific TitleCase MinIO names.
pub fn normalize_managed_metadata(
    metadata: &HashMap<String, String>,
    recode_kms_context: Option<KmsContextRecoder>,
) -> HashMap<String, String> {
    let mut normalized = metadata.clone();

    if !normalized.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER)
        && let Some(value) = metadata
            .get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER))
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER))
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER))
    {
        normalized.insert(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), value.clone());
    }

    if !normalized.contains_key(INTERNAL_ENCRYPTION_IV_HEADER)
        && let Some(value) = metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER)
    {
        normalized.insert(INTERNAL_ENCRYPTION_IV_HEADER.to_string(), value.clone());
    }

    if !normalized.contains_key(INTERNAL_ENCRYPTION_ALGORITHM_HEADER)
        && let Some(value) = metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER)
    {
        normalized.insert(INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), value.clone());
    }

    if !normalized.contains_key(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
        && let Some(value) = metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER)
    {
        normalized.insert(INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), value.clone());
    }

    if !normalized.contains_key(INTERNAL_ENCRYPTION_CONTEXT_HEADER)
        && let Some(value) = metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
        && let Some(recode) = recode_kms_context
        && let Some(encoded) = recode(value)
    {
        normalized.insert(INTERNAL_ENCRYPTION_CONTEXT_HEADER.to_string(), encoded);
    }

    normalized
}

/// Resolve the scheme and KMS key a stored managed-SSE object was wrapped with.
///
/// Mirrors the lookup `apply_managed_decryption_material` performs, so both agree on
/// which key a read is authorized against.
///
/// No [`KmsContextRecoder`] is taken: the context mapping only ever inserts
/// [`INTERNAL_ENCRYPTION_CONTEXT_HEADER`], which this lookup never reads, so
/// the result is identical with or without it.
pub fn stored_managed_encryption_key(metadata: &HashMap<String, String>) -> Option<(SSEType, String)> {
    if !contains_managed_encryption_metadata(metadata) {
        return None;
    }

    // Case-sensitive: the SSE writer stores the scheme under the lowercase
    // `x-amz-server-side-encryption` key; other casings are not stored forms.
    let sse_type = match metadata.get("x-amz-server-side-encryption")?.as_str() {
        AMZ_ENCRYPTION_KMS => SSEType::SseKms,
        _ => SSEType::SseS3,
    };
    let key_id = normalize_managed_metadata(metadata, None)
        .get(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
        .or_else(|| metadata.get("x-amz-server-side-encryption-aws-kms-key-id"))
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    Some((sse_type, key_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssec_transport_projection_retains_only_redacted_reader_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(SSEC_ALGORITHM_HEADER, http::HeaderValue::from_static("AES256"));
        headers.insert(SSEC_KEY_HEADER, http::HeaderValue::from_static("secret-key"));
        headers.insert(SSEC_KEY_MD5_HEADER, http::HeaderValue::from_static("key-md5"));
        headers.insert(http::header::AUTHORIZATION, http::HeaderValue::from_static("credential"));

        let projected = project_ssec_transport_headers(&headers);

        assert_eq!(projected.len(), 3);
        assert!(projected.values().all(http::HeaderValue::is_sensitive));
        assert!(projected.get(http::header::AUTHORIZATION).is_none());
        assert!(!format!("{projected:?}").contains("secret-key"));
    }

    #[test]
    fn transport_metadata_roundtrip_restores_stored_keys() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::HeaderName::from_static("x-rustfs-replication-ssec-algorithm"),
            http::HeaderValue::from_static("AES256"),
        );
        headers.insert(
            http::HeaderName::from_static("x-rustfs-replication-encryption-iv"),
            http::HeaderValue::from_static("iv-direct"),
        );
        headers.insert(
            http::HeaderName::from_static("x-rustfs-replication-server-side-encryption-iv"),
            http::HeaderValue::from_static("iv-minio"),
        );

        assert!(has_ssec_transport_headers(&headers));
        let restored = ssec_transport_to_stored_metadata(&headers).expect("transport headers must restore");
        // Restore MUST use the exact lowercase stored key: the read-path SSE-C
        // check is case-sensitive, so a TitleCase key would classify the
        // replica as managed-SSE and reject SSE-C GETs.
        assert_eq!(
            restored
                .get("x-amz-server-side-encryption-customer-algorithm")
                .map(String::as_str),
            Some("AES256")
        );
        assert!(!restored.keys().any(|k| k != "x-amz-server-side-encryption-customer-algorithm"
            && k.eq_ignore_ascii_case("x-amz-server-side-encryption-customer-algorithm")));
        assert_eq!(restored.get(INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str), Some("iv-direct"));
        assert_eq!(restored.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str), Some("iv-minio"));
        // The SSE marker is re-added to match the stored SSE-C shape.
        assert_eq!(restored.get("x-amz-server-side-encryption").map(String::as_str), Some("AES256"));

        let plain = http::HeaderMap::new();
        assert!(!has_ssec_transport_headers(&plain));
        assert!(ssec_transport_to_stored_metadata(&plain).is_none());
    }

    #[test]
    fn transport_lookup_is_case_insensitive() {
        assert_eq!(
            ssec_replication_transport_header("X-AMZ-SERVER-SIDE-ENCRYPTION-CUSTOMER-ALGORITHM"),
            Some(REPLICATION_SSEC_ALGORITHM_HEADER)
        );
        assert_eq!(
            ssec_replication_transport_header("x-minio-internal-server-side-encryption-sealed-key"),
            Some(REPLICATION_SSE_SEALED_KEY_HEADER)
        );
        assert_eq!(ssec_replication_transport_header("x-rustfs-encryption-key"), None);
    }

    #[test]
    fn stripped_predicate_covers_envelopes_intents_and_ssec_material() {
        // Managed-SSE envelope material (x-rustfs-encryption-* prefix).
        assert!(is_replication_stripped_encryption_key(INTERNAL_ENCRYPTION_KEY_HEADER));
        assert!(is_replication_stripped_encryption_key(INTERNAL_ENCRYPTION_KEY_ID_HEADER));
        assert!(is_replication_stripped_encryption_key(INTERNAL_ENCRYPTION_CONTEXT_HEADER));
        // MinIO-internal sealed material, including the managed rio-v2 keys
        // that only a non-default feature build ever writes — pinning them
        // here keeps the default CI honest about the full key population.
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER));
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER));
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER));
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER));
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER));
        assert!(is_replication_stripped_encryption_key(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER));
        // The dual-key invariant's rustfs-internal twin must be covered
        // standalone, without relying on an is_internal_key backstop.
        assert!(is_replication_stripped_encryption_key(
            "x-rustfs-internal-server-side-encryption-sealed-key"
        ));
        // SSE intent headers, including the KMS key id.
        assert!(is_replication_stripped_encryption_key("x-amz-server-side-encryption"));
        assert!(is_replication_stripped_encryption_key("x-amz-server-side-encryption-aws-kms-key-id"));
        assert!(is_replication_stripped_encryption_key(SSEC_ALGORITHM_HEADER));
        // is_sse_header does not cover the SSE-C original-size key; the
        // predicate must add it explicitly.
        assert!(is_replication_stripped_encryption_key(SSEC_ORIGINAL_SIZE_HEADER));
        assert!(is_replication_stripped_encryption_key(
            "X-Amz-Server-Side-Encryption-Customer-Original-Size"
        ));
        // Ordinary user metadata passes through.
        assert!(!is_replication_stripped_encryption_key("x-amz-meta-app"));
        assert!(!is_replication_stripped_encryption_key("content-type"));
    }

    #[test]
    fn managed_envelope_predicate_matches_both_key_families() {
        assert!(!contains_managed_encryption_metadata(&HashMap::new()));

        for key in [
            INTERNAL_ENCRYPTION_KEY_HEADER,
            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
            MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
            MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
            MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER,
        ] {
            let single = HashMap::from([(key.to_string(), "value".to_string())]);
            assert!(contains_managed_encryption_metadata(&single), "{key} must classify as managed SSE");
        }

        // SSE-C material alone is not a managed envelope.
        let ssec_only = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);
        assert!(!contains_managed_encryption_metadata(&ssec_only));
    }

    #[test]
    fn normalize_maps_minio_keys_onto_missing_rustfs_keys_only() {
        let metadata = HashMap::from([
            (MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), "minio-dek".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "minio-iv".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "DAREv2-HMAC-SHA256".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "minio-key".to_string()),
        ]);

        let normalized = normalize_managed_metadata(&metadata, None);
        assert_eq!(normalized.get(INTERNAL_ENCRYPTION_KEY_HEADER).map(String::as_str), Some("minio-dek"));
        assert_eq!(normalized.get(INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str), Some("minio-iv"));
        assert_eq!(
            normalized.get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
            Some("DAREv2-HMAC-SHA256")
        );
        assert_eq!(normalized.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER).map(String::as_str), Some("minio-key"));

        // Existing RustFS-branded keys always win over the MinIO twins.
        let mut both = metadata;
        both.insert(INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "rustfs-key".to_string());
        assert_eq!(
            normalize_managed_metadata(&both, None)
                .get(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
                .map(String::as_str),
            Some("rustfs-key")
        );

        // The mapping is a case-sensitive exact match on the TitleCase MinIO
        // names; a lowercased twin must not normalize.
        let lowercased = HashMap::from([(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_lowercase(), "minio-key".to_string())]);
        assert!(!normalize_managed_metadata(&lowercased, None).contains_key(INTERNAL_ENCRYPTION_KEY_ID_HEADER));
    }

    #[test]
    fn normalize_recodes_kms_context_only_through_the_injected_codec() {
        let metadata = HashMap::from([(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), "encoded-context".to_string())]);

        // Without a codec the context stays unnormalized.
        assert!(!normalize_managed_metadata(&metadata, None).contains_key(INTERNAL_ENCRYPTION_CONTEXT_HEADER));

        // A codec that fails to decode also leaves it unnormalized.
        fn reject(_value: &str) -> Option<String> {
            None
        }
        assert!(!normalize_managed_metadata(&metadata, Some(reject)).contains_key(INTERNAL_ENCRYPTION_CONTEXT_HEADER));

        fn recode(value: &str) -> Option<String> {
            Some(format!("recoded:{value}"))
        }
        assert_eq!(
            normalize_managed_metadata(&metadata, Some(recode))
                .get(INTERNAL_ENCRYPTION_CONTEXT_HEADER)
                .map(String::as_str),
            Some("recoded:encoded-context")
        );

        // A stored RustFS context wins without invoking the codec.
        let mut both = metadata;
        both.insert(INTERNAL_ENCRYPTION_CONTEXT_HEADER.to_string(), "stored-context".to_string());
        assert_eq!(
            normalize_managed_metadata(&both, Some(recode))
                .get(INTERNAL_ENCRYPTION_CONTEXT_HEADER)
                .map(String::as_str),
            Some("stored-context")
        );
    }

    #[test]
    fn stored_managed_encryption_key_attributes_scheme_and_key() {
        // Plaintext metadata carries no managed envelope.
        assert!(stored_managed_encryption_key(&HashMap::new()).is_none());

        // A managed envelope without the stored SSE marker cannot be attributed.
        let envelope_only = HashMap::from([(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "dek".to_string())]);
        assert!(stored_managed_encryption_key(&envelope_only).is_none());

        // The stored SSE marker is the lowercase form; a TitleCase key is not
        // a stored form and must not be recognized.
        let mut titlecase = envelope_only.clone();
        titlecase.insert("X-Amz-Server-Side-Encryption".to_string(), "aws:kms".to_string());
        assert!(stored_managed_encryption_key(&titlecase).is_none());

        let mut sse_s3 = envelope_only.clone();
        sse_s3.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
        assert_eq!(stored_managed_encryption_key(&sse_s3), Some((SSEType::SseS3, "default".to_string())));

        let mut sse_kms = envelope_only;
        sse_kms.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
        assert_eq!(stored_managed_encryption_key(&sse_kms), Some((SSEType::SseKms, "default".to_string())));

        // Key-id precedence: RustFS stored key id, then the MinIO twin, then
        // the lowercase amz key id, then "default".
        sse_kms.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), "amz-key".to_string());
        assert_eq!(stored_managed_encryption_key(&sse_kms), Some((SSEType::SseKms, "amz-key".to_string())));
        sse_kms.insert(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "minio-key".to_string());
        assert_eq!(stored_managed_encryption_key(&sse_kms), Some((SSEType::SseKms, "minio-key".to_string())));
        sse_kms.insert(INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "rustfs-key".to_string());
        assert_eq!(stored_managed_encryption_key(&sse_kms), Some((SSEType::SseKms, "rustfs-key".to_string())));
    }

    #[test]
    fn sse_type_audit_labels_are_stable() {
        assert_eq!(SSEType::SseS3.audit_label(), "SSE-S3");
        assert_eq!(SSEType::SseKms.audit_label(), "SSE-KMS");
        assert_eq!(SSEType::SseC.audit_label(), "SSE-C");
    }

    #[test]
    fn transport_prefixes_cover_every_transport_value_key() {
        // Every transport key that carries material must match a redaction
        // prefix; the multipart flag is a boolean marker and is exempt.
        for (_, transport) in SSEC_REPLICATION_TRANSPORT_HEADERS {
            if transport.eq_ignore_ascii_case(REPLICATION_ENCRYPTED_MULTIPART_HEADER) {
                continue;
            }
            let lower = transport.to_lowercase();
            assert!(
                REPLICATION_SSE_TRANSPORT_PREFIXES
                    .iter()
                    .any(|prefix| lower.starts_with(prefix)),
                "transport key {transport} is not covered by a redaction prefix"
            );
        }
    }
}
