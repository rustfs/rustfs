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
pub const NS_SCANNER_BODY_SHA256_QUERY: &str = "ns_scanner_body_sha256";
pub const NS_SCANNER_CAPABILITY_CHALLENGE_QUERY: &str = "ns_scanner_challenge";
pub const NS_SCANNER_CYCLE_QUERY: &str = "ns_scanner_cycle";
pub const NS_SCANNER_LEADER_EPOCH_QUERY: &str = "ns_scanner_leader_epoch";
pub const NS_SCANNER_REQUEST_ID_QUERY: &str = "ns_scanner_request_id";
pub const NS_SCANNER_SERVER_EPOCH_QUERY: &str = "ns_scanner_server_epoch";
pub const NS_SCANNER_SESSION_ID_QUERY: &str = "ns_scanner_session_id";
pub const NS_SCANNER_SESSION_SEQUENCE_QUERY: &str = "ns_scanner_session_sequence";
pub const NS_SCANNER_PROTOCOL_VERSION_QUERY: &str = "ns_scanner_protocol";
pub const NS_SCANNER_PROTOCOL_VERSION: u16 = 3;
pub const SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION: u32 = 0;
pub const SCANNER_ACTIVITY_PROTOCOL_VERSION: u32 = 4;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct NsScannerCapabilityResponse {
    pub version: u16,
    pub server_epoch: uuid::Uuid,
    pub proof: Vec<u8>,
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
pub use bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};
pub use capability::{CapabilitySnapshotError, CapabilityState, CapabilityStatus};
pub use error::{StorageErrorCode, StorageResult};
pub use multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};
pub use object::ObjectLockDeleteOptions;
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
pub use topology::{
    DiskCapabilities, TopologyCapabilities, TopologyDisk, TopologyLabels, TopologyPool, TopologySet, TopologySnapshot,
    TopologySnapshotProvider,
};
