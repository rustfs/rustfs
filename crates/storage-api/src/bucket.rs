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

use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Options for creating a new bucket.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MakeBucketOptions {
    /// Enable object lock for the bucket.
    pub lock_enabled: bool,
    /// Enable versioning for the bucket.
    pub versioning_enabled: bool,
    /// Force creation even if bucket already exists.
    pub force_create: bool,
    /// Optional creation timestamp.
    pub created_at: Option<OffsetDateTime>,
    /// Skip acquiring namespace lock.
    pub no_lock: bool,
}

/// Operation to perform on a bucket during site replication delete.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum SRBucketDeleteOp {
    /// No operation.
    #[default]
    NoOp,
    /// Mark the bucket for deletion.
    MarkDelete,
    /// Purge the bucket and all its contents.
    Purge,
}

/// Options for deleting a bucket.
#[derive(Debug, Default, Clone)]
pub struct DeleteBucketOptions {
    /// Skip acquiring namespace lock.
    pub no_lock: bool,
    /// Do not recreate bucket on failure.
    pub no_recreate: bool,
    /// Force deletion even if bucket is not empty.
    pub force: bool,
    /// Site replication delete operation.
    pub srdelete_op: SRBucketDeleteOp,
}

/// Options for querying bucket information.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BucketOptions {
    /// Include deleted buckets in results.
    pub deleted: bool,
    /// Use cached bucket information.
    pub cached: bool,
    /// Skip loading bucket metadata.
    pub no_metadata: bool,
}

/// Information about a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketInfo {
    /// Bucket name.
    pub name: String,
    /// Creation timestamp.
    pub created: Option<OffsetDateTime>,
    /// Deletion timestamp (if deleted).
    pub deleted: Option<OffsetDateTime>,
    /// Whether versioning is enabled.
    pub versioning: bool,
    /// Whether object locking is enabled.
    pub object_locking: bool,
}

/// Bucket-level storage operations.
#[async_trait::async_trait]
pub trait BucketOperations: Send + Sync + Debug {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<(), Self::Error>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo, Self::Error>;
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>, Self::Error>;
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_info_serializes_with_existing_fields() {
        let bucket = BucketInfo {
            name: "photos".to_owned(),
            created: Some(OffsetDateTime::UNIX_EPOCH),
            deleted: None,
            versioning: true,
            object_locking: false,
        };

        let encoded = serde_json::to_string(&bucket).expect("serialize BucketInfo to JSON");
        let decoded: BucketInfo = serde_json::from_str(&encoded).expect("deserialize BucketInfo from JSON");

        assert_eq!(decoded.name, "photos");
        assert_eq!(decoded.created, Some(OffsetDateTime::UNIX_EPOCH));
        assert!(decoded.versioning);
        assert!(!decoded.object_locking);
    }

    #[test]
    fn default_bucket_options_preserve_false_flags() {
        let opts = BucketOptions::default();

        assert!(!opts.deleted);
        assert!(!opts.cached);
        assert!(!opts.no_metadata);
    }

    #[test]
    fn default_delete_bucket_options_use_noop_site_replication() {
        let opts = DeleteBucketOptions::default();

        assert_eq!(opts.srdelete_op, SRBucketDeleteOp::NoOp);
        assert!(!opts.no_lock);
        assert!(!opts.no_recreate);
        assert!(!opts.force);
    }
}
