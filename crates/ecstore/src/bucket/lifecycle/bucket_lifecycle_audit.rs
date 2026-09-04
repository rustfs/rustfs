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

use super::runtime_boundary as runtime_sources;
use crate::bucket::lifecycle::lifecycle;
use crate::object_api::ObjectInfo;
use crate::services::event_notification::{EventArgs, send_event};
use crate::storage_api_contracts::object::{DeletedObject, ObjectToDelete};
use rustfs_s3_types::EventName;
use rustfs_scanner_metrics::metrics::IlmAction;

const LIFECYCLE_EXPIRY_USER_AGENT: &str = "Internal: [ILM-Expiry]";
const LIFECYCLE_TRANSITION_USER_AGENT: &str = "Internal: [ILM-Transition]";

#[derive(Debug, Clone, Default)]
pub enum LcEventSrc {
    #[default]
    None,
    Heal,
    Scanner,
    Decom,
    Rebal,
    S3HeadObject,
    S3GetObject,
    S3ListObjects,
    S3PutObject,
    S3CopyObject,
    S3CompleteMultipartUpload,
}

#[derive(Clone, Debug, Default)]
pub struct LcAuditEvent {
    pub event: lifecycle::Event,
    pub source: LcEventSrc,
}

impl LcAuditEvent {
    pub fn new(event: lifecycle::Event, source: LcEventSrc) -> Self {
        Self { event, source }
    }
}

pub(crate) fn emit_transition_failed_event(object: ObjectInfo) {
    emit_lifecycle_event(EventName::ObjectTransitionFailed, object, LIFECYCLE_TRANSITION_USER_AGENT);
}

pub(crate) fn emit_transition_complete_event(object: ObjectInfo) {
    emit_lifecycle_event(EventName::ObjectTransitionComplete, object, LIFECYCLE_TRANSITION_USER_AGENT);
}

pub(crate) fn emit_transitioned_expiration_event(source: &ObjectInfo, deleted: &ObjectInfo) {
    let event_name = transitioned_expiration_event_name(source.delete_marker, deleted.delete_marker);
    let object = ObjectInfo {
        bucket: source.bucket.clone(),
        name: source.name.clone(),
        size: source.size,
        version_id: source.version_id,
        delete_marker: source.delete_marker,
        ..Default::default()
    };
    emit_lifecycle_event(event_name, object, LIFECYCLE_EXPIRY_USER_AGENT);
}

pub(crate) fn emit_non_transitioned_expiration_event(action: IlmAction, source: &ObjectInfo, deleted: ObjectInfo) {
    let event_name = non_transitioned_expiration_event_name(action, source.delete_marker, deleted.delete_marker);
    emit_lifecycle_event(event_name, deleted, LIFECYCLE_EXPIRY_USER_AGENT);
}

/// Emit the lifecycle expiration event for one version removed by the batch
/// `NewerNoncurrentVersions` expiry path.
///
/// That path never sent events, so a successful noncurrent-version expiry was
/// invisible to notification subscribers even though the equivalent
/// current-version path emits one (backlog#2202).
pub(crate) fn emit_noncurrent_expiration_event(bucket: &str, target: &ObjectToDelete, deleted: &DeletedObject, failed: bool) {
    if let Some((event_name, object)) = noncurrent_expiration_event(bucket, target, deleted, failed) {
        emit_lifecycle_event(event_name, object, LIFECYCLE_EXPIRY_USER_AGENT);
    }
}

/// Decide which event a single batch entry earned, if any.
///
/// Only an entry that mutated something may be announced. "No error" is not
/// enough, and neither is `found`: the disk layer skips an absent version and
/// reports success (`delete_versions_internal` in `disk/local.rs` continues
/// past `FileVersionNotFound`), so a batch entry for a version that was
/// already gone comes back indistinguishable from a committed delete. The
/// delete plan's own source lookup is the signal that survives that, and the
/// lifecycle batch path always performs it because every target carries an
/// exact version identity.
fn noncurrent_expiration_event(
    bucket: &str,
    target: &ObjectToDelete,
    deleted: &DeletedObject,
    failed: bool,
) -> Option<(EventName, ObjectInfo)> {
    if failed || !deleted.found || deleted.source_missing {
        return None;
    }
    // A version removed by explicit version id is a plain versioned delete
    // even when that version is itself a delete marker; only a request that
    // carried no version id can publish a new delete marker. This is the rule
    // the S3 DeleteObjects path applies (issue #6745). `delete_object_versions`
    // now refuses targets without an exact version identity, so the
    // marker-creation shape is unreachable from that caller; the mapping stays
    // here so a future caller cannot silently announce the wrong mutation.
    let created_delete_marker = deleted.delete_marker && target.version_id.is_none();
    let (event_name, version_id) = if created_delete_marker {
        (EventName::LifecycleExpirationDeleteMarkerCreated, deleted.delete_marker_version_id)
    } else {
        (EventName::LifecycleExpirationDelete, deleted.version_id.or(target.version_id))
    };
    let object = ObjectInfo {
        bucket: bucket.to_string(),
        name: target.object_name.clone(),
        version_id,
        delete_marker: deleted.delete_marker,
        ..Default::default()
    };
    Some((event_name, object))
}

fn emit_lifecycle_event(event_name: EventName, object: ObjectInfo, user_agent: &str) {
    send_event(EventArgs {
        event_name: event_name.to_string(),
        bucket_name: object.bucket.clone(),
        object,
        user_agent: user_agent.to_string(),
        host: runtime_sources::default_local_node_name(),
        ..Default::default()
    });
}

fn transitioned_expiration_event_name(source_delete_marker: bool, deleted_delete_marker: bool) -> EventName {
    if source_delete_marker {
        EventName::LifecycleExpirationDelete
    } else if deleted_delete_marker {
        EventName::LifecycleExpirationDeleteMarkerCreated
    } else {
        EventName::LifecycleExpirationDelete
    }
}

fn non_transitioned_expiration_event_name(
    action: IlmAction,
    source_delete_marker: bool,
    deleted_delete_marker: bool,
) -> EventName {
    match action {
        IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => EventName::LifecycleExpirationDelete,
        _ if source_delete_marker => EventName::LifecycleExpirationDelete,
        _ if deleted_delete_marker => EventName::LifecycleExpirationDeleteMarkerCreated,
        _ => EventName::LifecycleExpirationDelete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn transitioned_expiration_event_marks_delete_marker_creation() {
        assert_eq!(
            transitioned_expiration_event_name(false, true),
            EventName::LifecycleExpirationDeleteMarkerCreated
        );
    }

    #[test]
    fn non_transitioned_delete_all_uses_delete_event() {
        assert_eq!(
            non_transitioned_expiration_event_name(IlmAction::DeleteAllVersionsAction, false, true),
            EventName::LifecycleExpirationDelete
        );
    }

    fn deleted_version(version_id: Uuid) -> DeletedObject {
        DeletedObject {
            object_name: "object".to_string(),
            version_id: Some(version_id),
            found: true,
            ..Default::default()
        }
    }

    fn target_version(version_id: Option<Uuid>) -> ObjectToDelete {
        ObjectToDelete {
            object_name: "object".to_string(),
            version_id,
            ..Default::default()
        }
    }

    #[test]
    fn noncurrent_expiration_emits_versioned_delete_with_exact_identity() {
        let version_id = Uuid::new_v4();
        let (event_name, object) =
            noncurrent_expiration_event("bucket", &target_version(Some(version_id)), &deleted_version(version_id), false)
                .expect("a committed delete must emit");
        assert_eq!(event_name, EventName::LifecycleExpirationDelete);
        assert_eq!(object.bucket, "bucket");
        assert_eq!(object.name, "object");
        assert_eq!(object.version_id, Some(version_id));
    }

    /// Removing a noncurrent version that happens to be a delete marker is a
    /// plain versioned delete, not a delete-marker creation.
    #[test]
    fn noncurrent_expiration_of_a_delete_marker_version_is_a_plain_delete() {
        let version_id = Uuid::new_v4();
        let deleted = DeletedObject {
            delete_marker: true,
            ..deleted_version(version_id)
        };
        let (event_name, object) = noncurrent_expiration_event("bucket", &target_version(Some(version_id)), &deleted, false)
            .expect("a committed delete must emit");
        assert_eq!(event_name, EventName::LifecycleExpirationDelete);
        assert_eq!(object.version_id, Some(version_id));
    }

    #[test]
    fn noncurrent_expiration_reports_a_created_delete_marker() {
        let marker_version_id = Uuid::new_v4();
        let deleted = DeletedObject {
            object_name: "object".to_string(),
            delete_marker: true,
            delete_marker_version_id: Some(marker_version_id),
            found: true,
            ..Default::default()
        };
        let (event_name, object) =
            noncurrent_expiration_event("bucket", &target_version(None), &deleted, false).expect("a committed delete must emit");
        assert_eq!(event_name, EventName::LifecycleExpirationDeleteMarkerCreated);
        assert_eq!(object.version_id, Some(marker_version_id));
    }

    /// A batch mixes successes with failures and versions that were already
    /// gone; only a real mutation may produce an event. A version that was
    /// already gone comes back with no error and `found` set, so
    /// `source_missing` is the signal that keeps it silent.
    #[test]
    fn noncurrent_expiration_skips_failed_and_missing_versions() {
        let version_id = Uuid::new_v4();
        let target = target_version(Some(version_id));
        assert!(noncurrent_expiration_event("bucket", &target, &deleted_version(version_id), true).is_none());

        let absent = DeletedObject {
            source_missing: true,
            ..deleted_version(version_id)
        };
        assert!(noncurrent_expiration_event("bucket", &target, &absent, false).is_none());

        let not_found = DeletedObject {
            found: false,
            ..deleted_version(version_id)
        };
        assert!(noncurrent_expiration_event("bucket", &target, &not_found, false).is_none());
    }
}
