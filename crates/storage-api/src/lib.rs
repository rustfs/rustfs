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
pub use object::{DeletedObject, ObjectToDelete};
pub use object::{ExpirationOptions, TransitionedObject};
pub use object::{HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, ObjectLockDeleteOptions, ObjectLockRetentionOptions};
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
