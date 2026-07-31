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

use crate::runtime_sources::current_object_store_handle;
use crate::storage_api::kms::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage_api::kms::{ECStore, StorageError, get_bucket_sse_config};
use async_trait::async_trait;
use rustfs_kms::DeletionReferenceChecker;
use s3s::dto::ServerSideEncryptionConfiguration;
use std::sync::Arc;
use tracing::warn;

/// Reference reported when bucket configuration cannot be inspected at all.
const BUCKET_CONFIG_UNAVAILABLE: &str = "bucket-encryption-config:unavailable";

/// Blocks deletion of keys referenced by any bucket's SSE configuration
/// (default bucket KMS key). Registered on the KMS service manager at startup
/// and consulted by the background deletion worker.
pub(crate) struct BucketEncryptionReferenceChecker;

#[async_trait]
impl DeletionReferenceChecker for BucketEncryptionReferenceChecker {
    async fn references(&self, key_id: &str) -> Vec<String> {
        collect_references(key_id, current_object_store_handle()).await
    }
}

async fn collect_references(key_id: &str, store: Option<Arc<ECStore>>) -> Vec<String> {
    // Fail closed: destroying key material is irreversible while the deletion
    // worker retries every sweep, so an uninspectable configuration must block
    // the removal rather than wave it through. This also covers the worker
    // racing server startup, before the object store is published.
    let Some(store) = store else {
        warn!(key_id, "KMS deletion reference check: object store not ready; blocking removal");
        return vec![BUCKET_CONFIG_UNAVAILABLE.to_string()];
    };
    let buckets = match store.list_bucket(&BucketOptions::default()).await {
        Ok(buckets) => buckets,
        Err(error) => {
            warn!(key_id, %error, "KMS deletion reference check: listing buckets failed; blocking removal");
            return vec![BUCKET_CONFIG_UNAVAILABLE.to_string()];
        }
    };

    let mut references = Vec::new();
    for bucket in &buckets {
        let lookup = get_bucket_sse_config(&bucket.name).await;
        references.extend(bucket_reference(&bucket.name, lookup, key_id));
    }
    references
}

/// `Some(reference)` when the bucket's encryption configuration references
/// `key_id` or cannot be read (fail closed); `None` when the bucket provably
/// does not reference the key.
fn bucket_reference(
    bucket: &str,
    lookup: Result<ServerSideEncryptionConfiguration, StorageError>,
    key_id: &str,
) -> Option<String> {
    match lookup {
        Ok(config) if sse_config_references_key(&config, key_id) => Some(format!("bucket:{bucket}")),
        Ok(_) => None,
        Err(StorageError::ConfigNotFound) => None,
        Err(error) => {
            warn!(
                bucket,
                key_id,
                %error,
                "KMS deletion reference check: unreadable bucket encryption config; blocking removal"
            );
            Some(format!("bucket:{bucket}:encryption-config-unreadable"))
        }
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
        assert_eq!(
            bucket_reference("sse-bucket", Ok(sse_kms_config(Some("kms-key-1"))), "kms-key-1"),
            Some("bucket:sse-bucket".to_string())
        );
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
        let reference = bucket_reference("broken", Err(StorageError::FaultyDisk), "kms-key-1");
        assert_eq!(reference, Some("bucket:broken:encryption-config-unreadable".to_string()));
    }

    #[tokio::test]
    async fn missing_object_store_blocks_deletion() {
        let references = collect_references("kms-key-1", None).await;
        assert_eq!(references, vec![BUCKET_CONFIG_UNAVAILABLE.to_string()]);
    }
}
