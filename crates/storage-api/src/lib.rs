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

//! Storage API contracts for RustFS.

pub const WALK_DIR_STREAM_COMPLETION_QUERY: &str = "walk_dir_stream_completion";
pub const WALK_DIR_STREAM_COMPLETION_V1: &str = "error-v1";
pub const WALK_DIR_BODY_SHA256_QUERY: &str = "walk_dir_body_sha256";
pub const PUT_FILE_AUTH_QUERY: &str = "put_file_auth";
pub const PUT_FILE_AUTH_V1: &str = "digest-trailer-v1";
pub const PUT_FILE_NONCE_QUERY: &str = "put_file_nonce";
pub const PUT_FILE_CAPABILITY_QUERY: &str = "put_file_capability";
pub const PUT_FILE_CAPABILITY_CHALLENGE_QUERY: &str = "put_file_challenge";
pub const PUT_FILE_CAPABILITY_VERSION: u16 = 1;
pub const PUT_FILE_SERVER_EPOCH_QUERY: &str = "put_file_server_epoch";
pub const PUT_FILE_AUTH_TRAILER_MAGIC: &[u8; 16] = b"RFS-PUT-AUTH-V1\0";
pub const PUT_FILE_AUTH_TRAILER_DIGEST_LEN: usize = 64;
pub const PUT_FILE_AUTH_TRAILER_MAC_LEN: usize = 32;
pub const PUT_FILE_AUTH_TRAILER_LEN: usize =
    PUT_FILE_AUTH_TRAILER_MAGIC.len() + PUT_FILE_AUTH_TRAILER_DIGEST_LEN + PUT_FILE_AUTH_TRAILER_MAC_LEN;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PutFileCapabilityResponse {
    pub version: u16,
    pub server_epoch: uuid::Uuid,
    pub proof: Vec<u8>,
}
pub const NS_SCANNER_BODY_SHA256_QUERY: &str = "ns_scanner_body_sha256";
pub const NS_SCANNER_CAPABILITY_CHALLENGE_QUERY: &str = "ns_scanner_challenge";
pub const NS_SCANNER_CYCLE_QUERY: &str = "ns_scanner_cycle";
pub const NS_SCANNER_LEADER_EPOCH_QUERY: &str = "ns_scanner_leader_epoch";
pub const NS_SCANNER_REQUEST_ID_QUERY: &str = "ns_scanner_request_id";
pub const NS_SCANNER_SERVER_EPOCH_QUERY: &str = "ns_scanner_server_epoch";
pub const NS_SCANNER_SESSION_ID_QUERY: &str = "ns_scanner_session_id";
pub const NS_SCANNER_SESSION_SEQUENCE_QUERY: &str = "ns_scanner_session_sequence";
pub const NS_SCANNER_PROTOCOL_VERSION_QUERY: &str = "ns_scanner_protocol";
pub const NS_SCANNER_COMPLETE_OBSERVATION_QUERY: &str = "ns_scanner_complete_observation";
pub const NS_SCANNER_DURABLE_PROGRESS_QUERY: &str = "ns_scanner_durable_progress";
pub const NS_SCANNER_CHECKPOINT_COVERAGE_QUERY: &str = "ns_scanner_checkpoint_coverage";
pub const NS_SCANNER_TIER_REGISTRY_GENERATION_QUERY: &str = "ns_scanner_tier_registry_generation";
pub const NS_SCANNER_PROTOCOL_VERSION: u16 = 3;
pub const SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION: u32 = 0;
pub const SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION: u32 = 5;
/// Protocol v6 carries the activity fields that predate the storage-owned
/// movement generation.  It remains readable during the rolling upgrade, but
/// a scanner must not use it as a publication proof because terminal movement
/// state is not authenticated by that version.
pub const SCANNER_ACTIVITY_V6_PROTOCOL_VERSION: u32 = 6;
/// Authenticates terminal movement state, but carries no checkpoint continuity.
pub const SCANNER_ACTIVITY_V7_PROTOCOL_VERSION: u32 = 7;
/// Authenticates read-only checkpoint continuity separately from the RPC epoch.
pub const SCANNER_ACTIVITY_PROTOCOL_VERSION: u32 = 8;
pub const SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION: u32 = 2;
pub const SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES: usize = 32;
pub const SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES: usize = SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES;
pub const SCANNER_DIRTY_USAGE_SNAPSHOT_RPC_MAX_MESSAGE_SIZE: usize = 512 * 1024;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct NsScannerCapabilityResponse {
    pub version: u16,
    pub server_epoch: uuid::Uuid,
    pub proof: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_tier_registry_generation: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_checkpoint_coverage: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_durable_progress: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_complete_observation: Option<bool>,
    /// Independent challenge-bound capability; omitted for older strict readers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_watch_proof: Option<Vec<u8>>,
}

/// Authenticated scanner capabilities for one peer startup epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NsScannerCapabilities {
    pub server_epoch: uuid::Uuid,
    pub supports_durable_progress: bool,
    pub supports_complete_observation: bool,
    pub supports_usage_watch: bool,
}

pub mod admin;
pub mod bucket;
pub mod capability;
pub mod error;
pub mod multipart;
pub mod object;
pub mod observability;
pub mod topology;

mod replication;

pub use admin::{DiskSetSelector, StorageAdminApi};
pub use bucket::BUCKET_LIFECYCLE_LOCK_OBJECT;
pub use bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};
pub use capability::{CapabilitySnapshotError, CapabilityState, CapabilityStatus};
pub use error::{StorageErrorCode, StorageResult};
pub use multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};
pub use multipart::{MultipartCompletionAuthorization, MultipartUploadInspection};
pub use object::AuthoritativeListOperations;
pub use object::DeleteAccounting;
pub use object::ObjectLockDeleteOptions;
pub use object::ObjectToDeleteIdentity;
pub use object::{DeletedObject, ObjectToDelete};
pub use object::{ExpirationOptions, TransitionedObject};
pub use object::{HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, ObjectLockRetentionOptions};
pub use object::{HealOperations, MultipartOperations, NamespaceLocking, ObjectIO, ObjectOperations};
pub use object::{ListObjectVersionsInfo, ListObjectsInfo, ListObjectsV2Info, ListOperations, ObjectInfoOrErr};
pub use object::{ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState};
pub use object::{VersionMarker, WalkOptions, WalkVersionsSortOrder};
pub use observability::{
    MemorySamplingState, ObservabilitySnapshot, ObservabilitySnapshotProvider, PlatformSupport, UserspaceProfilingCapability,
};
/// Object-metadata keys persisted in xl.meta `meta_user`. filemeta owns the
/// on-disk spelling; contract consumers that read or write persisted object
/// metadata (lifecycle object-lock checks) take the keys from here.
pub use rustfs_filemeta::metadata_keys;
pub use topology::{
    DiskCapabilities, TopologyCapabilities, TopologyDisk, TopologyLabels, TopologyPool, TopologySet, TopologySnapshot,
    TopologySnapshotProvider,
};
