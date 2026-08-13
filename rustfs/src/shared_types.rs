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

//! Layer-neutral shared types (backlog#1834).
//!
//! These types are consumed across the app, infra, and interface layers but
//! used to live under `server`, so every lower-layer import was an upward
//! edge that had to be baselined by the layer-dependency guard. `server`
//! re-exports them for its own consumers; new code should import from here.

use crate::storage_api::server::event::StorageObjectInfo;
use jiff::Timestamp;
use rustfs_notify::NotifyObjectInfo;

/// Peer address of the current request, injected as a request extension.
#[derive(Clone, Copy, Debug)]
pub struct RemoteAddr(pub std::net::SocketAddr);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DependencyReadiness {
    pub storage_ready: bool,
    pub iam_ready: bool,
    pub lock_quorum_ready: bool,
    pub peer_health_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessDegradedReason {
    StorageQuorumUnavailable,
    IamNotReady,
    LockQuorumUnavailable,
    KmsNotReady,
    ObjectReadStalled,
    ObjectWriteStalled,
    ClusterHealthTimeout,
    PeerHealthUnavailable,
    StorageAndIamUnavailable,
    StorageAndLockUnavailable,
    IamAndLockUnavailable,
    StorageIamAndLockUnavailable,
}

impl ReadinessDegradedReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadinessDegradedReason::StorageQuorumUnavailable => "storage_quorum_unavailable",
            ReadinessDegradedReason::IamNotReady => "iam_not_ready",
            ReadinessDegradedReason::LockQuorumUnavailable => "lock_quorum_unavailable",
            ReadinessDegradedReason::KmsNotReady => "kms_not_ready",
            ReadinessDegradedReason::ObjectReadStalled => "object_read_stalled",
            ReadinessDegradedReason::ObjectWriteStalled => "object_write_stalled",
            ReadinessDegradedReason::ClusterHealthTimeout => "cluster_health_timeout",
            ReadinessDegradedReason::PeerHealthUnavailable => "peer_health_unavailable",
            ReadinessDegradedReason::StorageAndIamUnavailable => "storage_and_iam_unavailable",
            ReadinessDegradedReason::StorageAndLockUnavailable => "storage_and_lock_unavailable",
            ReadinessDegradedReason::IamAndLockUnavailable => "iam_and_lock_unavailable",
            ReadinessDegradedReason::StorageIamAndLockUnavailable => "storage_iam_and_lock_unavailable",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyReadinessReport {
    pub readiness: DependencyReadiness,
    pub degraded_reasons: Vec<ReadinessDegradedReason>,
}

pub(crate) fn convert_ecstore_object_info(object: StorageObjectInfo) -> NotifyObjectInfo {
    NotifyObjectInfo {
        bucket: object.bucket,
        name: object.name,
        size: object.size,
        etag: object.etag,
        content_type: object.content_type,
        user_defined: object
            .user_defined
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        version_id: object.version_id.map(|version_id| version_id.to_string()),
        mod_time: object.mod_time.and_then(offset_date_time_to_timestamp),
        restore_expires: object.restore_expires.and_then(offset_date_time_to_timestamp),
        storage_class: object.storage_class,
        transitioned_tier: (!object.transitioned_object.tier.is_empty()).then_some(object.transitioned_object.tier),
    }
}

pub(crate) fn offset_date_time_to_timestamp(value: time::OffsetDateTime) -> Option<Timestamp> {
    let nanosecond = value.nanosecond().try_into().ok()?;
    Timestamp::new(value.unix_timestamp(), nanosecond).ok()
}
