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

//! Reference gate consulted before a scheduled KMS key deletion destroys
//! material: a key that any bucket's encryption configuration still points at
//! must not be removed. Object-level references (existing envelopes) are out
//! of scope here; those objects stay decryptable only until the key is gone,
//! which is why the pending-deletion window exists.
//!
//! The same collection also backs the impact section of the admin key
//! responses, so an operator sees the references that will block a deletion at
//! the moment they schedule it rather than only in a server-side log once the
//! window has run out. Both consumers read the same collection: the report
//! cannot describe a deployment the gate does not enforce.
//!
//! Cost is bounded by the number of buckets — one bucket listing plus one
//! cached metadata lookup each. Nothing here lists objects or versions.

use crate::runtime_sources::current_object_store_handle;
use crate::storage_api::kms::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage_api::kms::{ECStore, StorageError, get_bucket_sse_config};
use async_trait::async_trait;
use rustfs_kms::{DeletionReferenceChecker, KeyImpactReport, KeyReference, KeyReferenceKind};
use s3s::dto::ServerSideEncryptionConfiguration;
use std::sync::Arc;
use tracing::warn;

/// Source name reported when bucket configuration cannot be inspected at all.
const BUCKET_ENCRYPTION_SOURCE: &str = "bucket-encryption-config";

/// Blocks deletion of keys referenced by any bucket's SSE configuration
/// (default bucket KMS key). Registered on the KMS service manager at startup
/// and consulted by the background deletion worker.
pub(crate) struct BucketEncryptionReferenceChecker;

#[async_trait]
impl DeletionReferenceChecker for BucketEncryptionReferenceChecker {
    async fn references(&self, key_id: &str) -> Vec<String> {
        // Bucket configuration only, and no service default key: the deletion
        // worker checks the default key itself before it consults a checker,
        // so this reports exactly the reference set it always has.
        collect_key_impact(key_id, None, current_object_store_handle())
            .await
            .references
            .iter()
            .map(blocking_reference)
            .collect()
    }
}

/// Configuration-layer impact of deleting `key_id`.
///
/// Exhaustive over what it covers, and explicit about what it does not:
/// object envelopes written under the key are not enumerated, so an empty
/// reference list is never a statement that the key is unused.
pub(crate) async fn current_key_impact(key_id: &str, default_key_id: Option<&str>) -> KeyImpactReport {
    collect_key_impact(key_id, default_key_id, current_object_store_handle()).await
}

async fn collect_key_impact(key_id: &str, default_key_id: Option<&str>, store: Option<Arc<ECStore>>) -> KeyImpactReport {
    let mut report = KeyImpactReport::configuration_layer(key_id);

    if default_key_id == Some(key_id) {
        report.push_reference(KeyReference {
            kind: KeyReferenceKind::ServiceDefaultKey,
            id: key_id.to_string(),
            detail: "the KMS service is configured to use this key as its default key".to_string(),
        });
    }

    // Fail closed: destroying key material is irreversible while the deletion
    // worker retries every sweep, so an uninspectable configuration must block
    // the removal rather than wave it through. This also covers the worker
    // racing server startup, before the object store is published.
    let Some(store) = store else {
        warn!(key_id, "KMS deletion reference check: object store not ready; blocking removal");
        report.push_reference(unreadable_source(
            "object store is not ready, so bucket encryption configuration could not be read",
        ));
        return report;
    };
    let buckets = match store.list_bucket(&BucketOptions::default()).await {
        Ok(buckets) => buckets,
        Err(error) => {
            warn!(key_id, %error, "KMS deletion reference check: listing buckets failed; blocking removal");
            report.push_reference(unreadable_source(format!("buckets could not be listed: {error}")));
            return report;
        }
    };

    for bucket in &buckets {
        let lookup = get_bucket_sse_config(&bucket.name).await;
        if let Some(reference) = bucket_reference(&bucket.name, lookup, key_id) {
            report.push_reference(reference);
        }
    }
    report
}

fn unreadable_source(detail: impl Into<String>) -> KeyReference {
    KeyReference {
        kind: KeyReferenceKind::UnreadableSource,
        id: BUCKET_ENCRYPTION_SOURCE.to_string(),
        detail: detail.into(),
    }
}

/// `Some(reference)` when the bucket's encryption configuration references
/// `key_id` or cannot be read (fail closed); `None` when the bucket provably
/// does not reference the key.
fn bucket_reference(
    bucket: &str,
    lookup: Result<ServerSideEncryptionConfiguration, StorageError>,
    key_id: &str,
) -> Option<KeyReference> {
    match lookup {
        Ok(config) if sse_config_references_key(&config, key_id) => Some(KeyReference {
            kind: KeyReferenceKind::BucketDefaultEncryption,
            id: bucket.to_string(),
            detail: "bucket default encryption names this key, so new objects are written under it".to_string(),
        }),
        Ok(_) => None,
        Err(StorageError::ConfigNotFound) => None,
        Err(error) => {
            warn!(
                bucket,
                key_id,
                %error,
                "KMS deletion reference check: unreadable bucket encryption config; blocking removal"
            );
            Some(KeyReference {
                kind: KeyReferenceKind::UnreadableResource,
                id: bucket.to_string(),
                detail: format!("bucket encryption configuration could not be read: {error}"),
            })
        }
    }
}

/// Reference identifier handed to the deletion worker.
///
/// The worker only needs an identifier to log and to count as non-empty, so
/// these strings stay exactly as they were before the structured report
/// existed; a reference is a reference regardless of how it renders.
fn blocking_reference(reference: &KeyReference) -> String {
    match reference.kind {
        KeyReferenceKind::BucketDefaultEncryption => format!("bucket:{}", reference.id),
        KeyReferenceKind::UnreadableResource => format!("bucket:{}:encryption-config-unreadable", reference.id),
        KeyReferenceKind::UnreadableSource => format!("{}:unavailable", reference.id),
        KeyReferenceKind::ServiceDefaultKey => format!("kms-service-default-key:{}", reference.id),
    }
}

fn sse_config_references_key(config: &ServerSideEncryptionConfiguration, key_id: &str) -> bool {
    config.rules.iter().any(|rule| {
        rule.apply_server_side_encryption_by_default
            .as_ref()
            .and_then(|sse| sse.kms_master_key_id.as_deref())
            .is_some_and(|configured| configured == key_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_kms::ReferenceCompleteness;
    use s3s::dto::{ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionRule};

    fn sse_kms_config(key_id: Option<&str>) -> ServerSideEncryptionConfiguration {
        ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                    kms_master_key_id: key_id.map(str::to_string),
                }),
                bucket_key_enabled: None,
            }],
        }
    }

    #[test]
    fn referencing_bucket_blocks_deletion() {
        let reference = bucket_reference("sse-bucket", Ok(sse_kms_config(Some("kms-key-1"))), "kms-key-1")
            .expect("a bucket that names the key must be reported");
        assert_eq!(reference.kind, KeyReferenceKind::BucketDefaultEncryption);
        assert_eq!(reference.id, "sse-bucket");
    }

    #[test]
    fn non_referencing_buckets_allow_deletion() {
        // Different key, no configured key, and no SSE configuration at all.
        assert_eq!(bucket_reference("other-key", Ok(sse_kms_config(Some("kms-key-2"))), "kms-key-1"), None);
        assert_eq!(bucket_reference("no-key", Ok(sse_kms_config(None)), "kms-key-1"), None);
        assert_eq!(bucket_reference("plain", Err(StorageError::ConfigNotFound), "kms-key-1"), None);
    }

    #[test]
    fn unreadable_config_blocks_deletion() {
        let reference = bucket_reference("broken", Err(StorageError::FaultyDisk), "kms-key-1")
            .expect("an unreadable bucket must be reported");
        assert_eq!(reference.kind, KeyReferenceKind::UnreadableResource);
        assert_eq!(reference.id, "broken");
    }

    /// The deletion worker's gate is the only thing standing between an
    /// expired key and destroyed material; reshaping the collection it reads
    /// must not change a single identifier it receives. The report is the
    /// second reading of that same collection, so it must never look clear
    /// where the gate objects.
    #[test]
    fn worker_reference_identifiers_are_unchanged() {
        let cases = [
            (
                bucket_reference("sse-bucket", Ok(sse_kms_config(Some("kms-key-1"))), "kms-key-1"),
                "bucket:sse-bucket",
            ),
            (
                bucket_reference("broken", Err(StorageError::FaultyDisk), "kms-key-1"),
                "bucket:broken:encryption-config-unreadable",
            ),
            (
                Some(unreadable_source("object store is not ready")),
                "bucket-encryption-config:unavailable",
            ),
        ];

        for (reference, expected) in cases {
            let reference = reference.expect("case must produce a reference");
            assert_eq!(blocking_reference(&reference), expected);

            let mut report = KeyImpactReport::configuration_layer("kms-key-1");
            report.push_reference(reference);
            assert!(report.blocks_destruction(), "{expected} must read as blocking in the report too");
        }
    }

    #[tokio::test]
    async fn missing_object_store_blocks_deletion() {
        let references: Vec<String> = collect_key_impact("kms-key-1", None, None)
            .await
            .references
            .iter()
            .map(blocking_reference)
            .collect();
        assert_eq!(references, vec!["bucket-encryption-config:unavailable".to_string()]);
    }

    /// An unreachable object store must never render as "we looked and found
    /// nothing": the report says the configuration could not be read.
    #[tokio::test]
    async fn missing_object_store_reports_unavailable_completeness() {
        let report = collect_key_impact("kms-key-1", None, None).await;

        assert_eq!(report.completeness, ReferenceCompleteness::Unavailable);
        assert!(report.blocks_destruction());
        assert_eq!(report.references.len(), 1);
        assert_eq!(report.references[0].kind, KeyReferenceKind::UnreadableSource);
    }

    /// The service default key is a configuration reference in its own right,
    /// and it is decidable without touching the object store.
    #[tokio::test]
    async fn service_default_key_is_reported_before_any_store_lookup() {
        let report = collect_key_impact("kms-key-1", Some("kms-key-1"), None).await;

        assert_eq!(report.references[0].kind, KeyReferenceKind::ServiceDefaultKey);
        assert_eq!(report.references[0].id, "kms-key-1");

        let other = collect_key_impact("kms-key-1", Some("kms-key-2"), None).await;
        assert!(
            !other
                .references
                .iter()
                .any(|reference| reference.kind == KeyReferenceKind::ServiceDefaultKey)
        );
    }
}
