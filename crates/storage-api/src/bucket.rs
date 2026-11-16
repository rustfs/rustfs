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

/// Namespace-lock object reserved for bucket lifecycle generation fencing.
/// Empty S3 object keys are invalid, so this cannot collide with an object lock.
pub const BUCKET_LIFECYCLE_LOCK_OBJECT: &str = "";

/// Persisted bucket family. Unknown serialized values are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BucketKind {
    General,
    Table,
    Vector,
}

/// Invalid identity syntax or an operation outside a managed object scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketIdentityError {
    InvalidAccountNamespaceId,
    InvalidRegion,
    InvalidPublicName,
    InvalidStorageBucketId,
    NilIncarnation,
    InvalidTableId,
    InvalidPrefix,
    EmptyOperationScope,
    ObjectOutsideScope,
    OperationNotAllowed,
}

impl std::fmt::Display for BucketIdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidAccountNamespaceId => "account namespace must be empty",
            Self::InvalidRegion => "invalid bucket region",
            Self::InvalidPublicName => "invalid public bucket name",
            Self::InvalidStorageBucketId => "invalid storage bucket identifier",
            Self::NilIncarnation => "bucket incarnation must not be nil",
            Self::InvalidTableId => "table identifier must not be empty",
            Self::InvalidPrefix => "invalid managed object prefix",
            Self::EmptyOperationScope => "managed object scope has no operations",
            Self::ObjectOutsideScope => "object is outside the managed prefix",
            Self::OperationNotAllowed => "operation is outside the managed scope",
        };
        f.write_str(message)
    }
}

impl std::error::Error for BucketIdentityError {}

fn valid_identity_name(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value != "."
        && value != ".."
        && !value.chars().any(|c| c.is_control() || matches!(c, '/' | '\\'))
}

/// Empty account component used by this deployment, independent of IAM access keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountNamespaceId(String);

impl AccountNamespaceId {
    pub const fn empty() -> Self {
        Self(String::new())
    }

    /// # Errors
    /// Rejects nonempty account components.
    pub fn try_new(value: String) -> Result<Self, BucketIdentityError> {
        if !value.is_empty() {
            return Err(BucketIdentityError::InvalidAccountNamespaceId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Public bucket name, separate from its physical storage identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketPublicName(String);

impl BucketPublicName {
    /// # Errors
    /// Rejects empty, oversized or path-containing names. Protocol validation is caller-owned.
    pub fn try_new(value: String) -> Result<Self, BucketIdentityError> {
        if !valid_identity_name(&value, 63) {
            return Err(BucketIdentityError::InvalidPublicName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Physical bucket identifier consumed by storage operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageBucketId(String);

impl StorageBucketId {
    /// # Errors
    /// Rejects empty, oversized or path-containing identifiers.
    pub fn try_new(value: String) -> Result<Self, BucketIdentityError> {
        if !valid_identity_name(&value, 63) {
            return Err(BucketIdentityError::InvalidStorageBucketId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Non-reusable bucket lifetime identity. UUID ordering has no meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BucketIncarnation(uuid::Uuid);

impl BucketIncarnation {
    /// # Errors
    /// Rejects the nil UUID.
    pub fn try_new(value: uuid::Uuid) -> Result<Self, BucketIdentityError> {
        if value.is_nil() {
            return Err(BucketIdentityError::NilIncarnation);
        }
        Ok(Self(value))
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }
}

/// Catalog table identity; legacy nonempty identities remain readable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManagedTableId(String);

impl ManagedTableId {
    /// # Errors
    /// Rejects empty identifiers. Resource owners validate new identities and encode paths.
    pub fn try_new(value: String) -> Result<Self, BucketIdentityError> {
        if value.is_empty() {
            return Err(BucketIdentityError::InvalidTableId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Complete bucket identity data. Construction validates syntax, not persisted state or authorization.
/// Consumers must resolve and revalidate this binding under the operation's lifecycle protection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketHandle {
    kind: BucketKind,
    account_namespace_id: AccountNamespaceId,
    region: String,
    public_name: BucketPublicName,
    storage_bucket_id: StorageBucketId,
    incarnation: BucketIncarnation,
}

impl BucketHandle {
    /// # Errors
    /// Rejects an empty or malformed region. This method performs no I/O or authorization.
    pub fn try_from_parts(
        kind: BucketKind,
        account_namespace_id: AccountNamespaceId,
        region: String,
        public_name: BucketPublicName,
        storage_bucket_id: StorageBucketId,
        incarnation: BucketIncarnation,
    ) -> Result<Self, BucketIdentityError> {
        if !valid_identity_name(&region, 255) || region.chars().any(char::is_whitespace) {
            return Err(BucketIdentityError::InvalidRegion);
        }
        Ok(Self {
            kind,
            account_namespace_id,
            region,
            public_name,
            storage_bucket_id,
            incarnation,
        })
    }

    pub fn kind(&self) -> BucketKind {
        self.kind
    }
    pub fn account_namespace_id(&self) -> &AccountNamespaceId {
        &self.account_namespace_id
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn public_name(&self) -> &BucketPublicName {
        &self.public_name
    }
    pub fn storage_bucket_id(&self) -> &StorageBucketId {
        &self.storage_bucket_id
    }
    pub fn incarnation(&self) -> BucketIncarnation {
        self.incarnation
    }
}

/// Individual operations supported by bounded managed object I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ManagedObjectOperation {
    CreateOnly,
    Get,
    Head,
    Range,
    CreateMultipart,
    UploadPart,
    CompleteMultipart,
    AbortMultipart,
}

/// Pure scope data. Possessing this value does not authorize an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedObjectAccess {
    bucket: BucketHandle,
    table_id: ManagedTableId,
    allowed_prefix: String,
    allowed_operations: std::collections::BTreeSet<ManagedObjectOperation>,
}

fn valid_managed_path(value: &str) -> bool {
    !value.is_empty()
        && !value.chars().any(|c| c.is_control() || c == '\\')
        && value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

impl ManagedObjectAccess {
    /// # Errors
    /// Rejects a non-relative prefix, unsafe segments or an empty operation set.
    /// The application must additionally verify table ownership and authorization.
    pub fn try_from_parts(
        bucket: BucketHandle,
        table_id: ManagedTableId,
        allowed_prefix: String,
        allowed_operations: std::collections::BTreeSet<ManagedObjectOperation>,
    ) -> Result<Self, BucketIdentityError> {
        if !allowed_prefix.strip_suffix('/').is_some_and(valid_managed_path) {
            return Err(BucketIdentityError::InvalidPrefix);
        }
        if allowed_operations.is_empty() {
            return Err(BucketIdentityError::EmptyOperationScope);
        }
        Ok(Self {
            bucket,
            table_id,
            allowed_prefix,
            allowed_operations,
        })
    }

    /// # Errors
    /// Rejects an unsafe key, a key outside the complete prefix, or an ungranted operation.
    pub fn check(&self, key: &str, operation: ManagedObjectOperation) -> Result<(), BucketIdentityError> {
        if !self.allowed_operations.contains(&operation) {
            return Err(BucketIdentityError::OperationNotAllowed);
        }
        if !valid_managed_path(key) || !key.starts_with(&self.allowed_prefix) || key.len() == self.allowed_prefix.len() {
            return Err(BucketIdentityError::ObjectOutsideScope);
        }
        Ok(())
    }

    pub fn bucket(&self) -> &BucketHandle {
        &self.bucket
    }
    pub fn table_id(&self) -> &ManagedTableId {
        &self.table_id
    }
    pub fn allowed_prefix(&self) -> &str {
        &self.allowed_prefix
    }
}

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
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DeleteBucketOptions {
    /// Skip acquiring namespace lock.
    pub no_lock: bool,
    /// Do not recreate bucket on failure.
    pub no_recreate: bool,
    /// Force deletion even if bucket is not empty.
    pub force: bool,
    /// Delete empty directory remnants after the local peer verifies no object metadata exists.
    pub force_if_empty: bool,
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

    fn managed_test_handle() -> BucketHandle {
        BucketHandle::try_from_parts(
            BucketKind::Vector,
            AccountNamespaceId::empty(),
            "us-east-1".into(),
            BucketPublicName::try_new("vectors".into()).expect("valid public name"),
            StorageBucketId::try_new("rustfs-v-test".into()).expect("valid storage ID"),
            BucketIncarnation::try_new(uuid::Uuid::new_v4()).expect("valid incarnation"),
        )
        .expect("valid handle")
    }

    #[test]
    fn identity_types_reject_invalid_values() {
        assert_eq!(
            AccountNamespaceId::try_new(String::new()).expect("empty account"),
            AccountNamespaceId::empty()
        );
        assert_eq!(AccountNamespaceId::empty().as_str(), "");
        for value in ["0", "123", "123456789012", "000000000000", " "] {
            assert!(AccountNamespaceId::try_new(value.into()).is_err());
        }
        assert_eq!(BucketIncarnation::try_new(uuid::Uuid::nil()), Err(BucketIdentityError::NilIncarnation));
        assert!(ManagedTableId::try_new(String::new()).is_err());
        assert_eq!(ManagedTableId::try_new("table-id".into()).expect("legacy identity").as_str(), "table-id");
        for name in ["", "../bucket", "a\\b", ".", "..", "a\0b"] {
            assert!(StorageBucketId::try_new(name.into()).is_err());
            assert!(BucketPublicName::try_new(name.into()).is_err());
        }
        assert!(serde_json::from_str::<BucketKind>("\"unknown\"").is_err());
        assert_eq!(serde_json::to_string(&BucketKind::Vector).expect("serialize kind"), "\"vector\"");
    }

    #[test]
    fn managed_scope_checks_segment_and_operation() {
        let scope = ManagedObjectAccess::try_from_parts(
            managed_test_handle(),
            ManagedTableId::try_new("table-id".into()).expect("legacy ID"),
            "indexes/table-id/".into(),
            [ManagedObjectOperation::Get].into_iter().collect(),
        )
        .expect("valid scope");
        assert_eq!(scope.check("indexes/table-id/1.manifest", ManagedObjectOperation::Get), Ok(()));
        assert_eq!(
            scope.check("indexes/table-id/1.manifest", ManagedObjectOperation::Range),
            Err(BucketIdentityError::OperationNotAllowed)
        );
        for key in [
            "indexes/table-id/",
            "indexes/table-id2/1.manifest",
            "indexes/table-id/../other",
            "/indexes/table-id/1",
            "indexes/table-id//1",
        ] {
            assert_eq!(
                scope.check(key, ManagedObjectOperation::Get),
                Err(BucketIdentityError::ObjectOutsideScope)
            );
        }
        for prefix in ["", "/indexes/", "indexes", "indexes/../", "indexes//"] {
            assert!(
                ManagedObjectAccess::try_from_parts(
                    managed_test_handle(),
                    scope.table_id().clone(),
                    prefix.into(),
                    [ManagedObjectOperation::Get].into_iter().collect()
                )
                .is_err()
            );
        }
    }

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
