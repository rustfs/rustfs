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
use rustfs_common::metrics::IlmAction;
use rustfs_s3_types::EventName;

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
}
