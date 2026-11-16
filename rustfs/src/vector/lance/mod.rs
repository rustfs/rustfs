// Copyright 2026 RustFS Team
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

mod commit;
mod dataset;
mod files;
mod filter;
mod initial;
mod manifest;
mod object_store;
mod schema;

pub(crate) use commit::{
    CommittedManifestInfo, DatasetSnapshot, LanceManifestAdapter, LanceOperationContext, ManifestPrepareRequest,
};
pub(crate) use dataset::{
    DatasetError, ProjectedVector, QueryCandidate, QueryMatch, VectorProjection, VectorRecord, validate_record_keys,
};
pub(crate) use initial::{
    InitialManifestPlan, decode_initialization_payload, encode_initialization_payload, plan_initial_manifest,
};
#[cfg(test)]
pub(crate) use schema::VectorDistanceMetric;
pub(crate) use schema::VectorIndexManifestSpec;

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::Range,
    sync::Arc,
};

use crate::storage_api::vector::ManagedObjectAccess;
use bytes::Bytes;
use uuid::Uuid;

use crate::table_catalog::{CatalogObjectKey, TableCatalogObject, TableCatalogObjectMetadata};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ManifestIoLimits {
    pub max_manifest_bytes: NonZeroU64,
    pub max_concurrent_reads: NonZeroUsize,
}

#[async_trait::async_trait]
pub(crate) trait ManagedManifestObjects: Send + Sync {
    async fn create_only(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
        content: Bytes,
    ) -> Result<TableCatalogObjectMetadata, LanceManifestError>;

    async fn create_multipart(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
    ) -> Result<Arc<dyn ManagedDatasetMultipart>, LanceManifestError>;

    async fn get(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
        range: Option<Range<u64>>,
        expected_body_revision: Option<Uuid>,
    ) -> Result<TableCatalogObject, LanceManifestError>;

    async fn head(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
    ) -> Result<TableCatalogObjectMetadata, LanceManifestError>;
}

/// An upload bound to one authorized object and its operation guard.
///
/// Accepted writes must retain the complete Catalog operation guard until their
/// storage work finishes, including when the caller is cancelled. Completion
/// returns the actual committed identity, and successful completion is retryable.
/// Abort only removes this upload's staging data and preserves a completed object.
#[async_trait::async_trait]
pub(crate) trait ManagedDatasetMultipart: Send + Sync {
    async fn put_part(&self, part_number: u16, content: Vec<Bytes>, size: u64) -> Result<(), LanceManifestError>;
    async fn complete(&self, expected_parts: u16) -> Result<TableCatalogObjectMetadata, LanceManifestError>;
    async fn abort(&self) -> Result<(), LanceManifestError>;
}

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub(crate) enum LanceManifestError {
    #[error("corrupt Lance manifest: {reason}")]
    CorruptManifest { reason: &'static str },
    #[error("unsupported Lance format: {reason}")]
    UnsupportedFormat { reason: &'static str },
    #[error("Lance schema mismatch: {reason}")]
    SchemaMismatch { reason: &'static str },
    #[error("Lance resource binding mismatch: {reason}")]
    ResourceMismatch { reason: &'static str },
    #[error("Lance manifest version mismatch: expected {expected}, found {actual}")]
    VersionMismatch { expected: u64, actual: u64 },
    #[error("Lance manifest SHA-256 does not match its reference")]
    DigestMismatch,
    #[error("Lance manifest length mismatch: expected {expected}, found {actual}")]
    LengthMismatch { expected: u64, actual: u64 },
    #[error("Lance manifest length {length} exceeds the read limit {limit}")]
    ReadLimitExceeded { length: u64, limit: u64 },
    #[error("Lance operation {operation} failed")]
    Engine {
        operation: &'static str,
        #[source]
        source: lance_core::Error,
    },
    #[error("Lance memory object operation failed")]
    ObjectStore(#[from] object_store_lance::Error),
    #[error("Lance manifest protobuf decoding failed")]
    Protobuf {
        #[source]
        source: prost::DecodeError,
    },
    #[error("Lance format descriptor JSON is invalid")]
    DescriptorJson(#[from] serde_json::Error),
    #[error("Lance version {version} is not committed")]
    VersionNotCommitted { version: u64 },
    #[error("the expected Catalog head changed before Lance publication")]
    ExpectedHeadConflict,
    #[error("Lance catalog operation failed")]
    Catalog(#[from] crate::table_catalog::TableCatalogStoreError),
    #[error("managed Lance object access was denied")]
    ManagedAccessDenied(#[from] crate::storage_api::vector::BucketIdentityError),
    #[error("Lance object reference mismatch: {reason}")]
    ReferenceMismatch { reason: &'static str },
    #[error("Lance candidate already exists: {key}")]
    CandidateAlreadyExists { key: String },
    #[error("Lance object was not found: {key}")]
    ObjectNotFound { key: String },
    #[error("managed object operation {operation} failed")]
    Storage {
        operation: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl LanceManifestError {
    fn engine(operation: &'static str, source: lance_core::Error) -> Self {
        Self::Engine { operation, source }
    }
}
