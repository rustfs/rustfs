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

use super::headers::{AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5};

pub const INTERNAL_ENCRYPTION_KEY_ID_HEADER: &str = "x-rustfs-encryption-key-id";
pub const INTERNAL_ENCRYPTION_KEY_HEADER: &str = "x-rustfs-encryption-key";
pub const INTERNAL_ENCRYPTION_IV_HEADER: &str = "x-rustfs-encryption-iv";
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
    (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, REPLICATION_SSEC_ALGORITHM_HEADER),
    (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, REPLICATION_SSEC_KEY_MD5_HEADER),
    (SSEC_ORIGINAL_SIZE_HEADER, REPLICATION_SSEC_ORIGINAL_SIZE_HEADER),
    (INTERNAL_ENCRYPTION_IV_HEADER, REPLICATION_ENCRYPTION_IV_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_IV_HEADER, REPLICATION_SSE_IV_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, REPLICATION_SSE_SEAL_ALGORITHM_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, REPLICATION_SSE_SEALED_KEY_HEADER),
    (MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER, REPLICATION_ENCRYPTED_MULTIPART_HEADER),
];

/// Prefixes of replication SSE transport keys whose values carry encryption
/// material and must never reach logs. Consumed by `rustfs_filemeta` redaction.
pub const REPLICATION_SSE_TRANSPORT_PREFIXES: &[&str] = &[
    "x-rustfs-replication-server-side-encryption-",
    "x-rustfs-replication-encryption-",
    "x-rustfs-replication-ssec-",
];

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
    // The dual-key invariant writes an x-rustfs-internal- twin next to every
    // x-minio-internal- SSE key; cover it here so this predicate is safe to
    // use standalone, without an is_internal_key backstop.
    super::is_encryption_metadata_key(key)
        || super::is_sse_header(key)
        || key.eq_ignore_ascii_case(SSEC_ORIGINAL_SIZE_HEADER)
        || super::starts_with_ignore_ascii_case(key, "x-rustfs-internal-server-side-encryption-")
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(is_replication_stripped_encryption_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM));
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
