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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MakeBucketOptions {
    pub lock_enabled: bool,
    pub versioning_enabled: bool,
    pub force_create: bool,
    pub created_at: Option<OffsetDateTime>,
    pub no_lock: bool,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum SRBucketDeleteOp {
    #[default]
    NoOp,
    MarkDelete,
    Purge,
}

#[derive(Debug, Default, Clone)]
pub struct DeleteBucketOptions {
    pub no_lock: bool,
    pub no_recreate: bool,
    pub force: bool,
    pub srdelete_op: SRBucketDeleteOp,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BucketOptions {
    pub deleted: bool,
    pub cached: bool,
    pub no_metadata: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
    pub deleted: Option<OffsetDateTime>,
    pub versioning: bool,
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
