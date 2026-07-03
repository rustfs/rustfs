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

use std::collections::HashMap;

use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER,
    SUFFIX_REPLICATION_RESET_STATUS, get_header_map,
};
use s3s::dto::ReplicationConfiguration;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::storage_api::ObjectToDelete;
use crate::{
    ObjectOpts, ReplicateDecision, ReplicateTargetDecision, ReplicationConfigurationExt as _, ReplicationState,
    ReplicationStatusType, ReplicationType, ResyncTargetDecision, VersionPurgeStatusType, replication_statuses_map,
    target_reset_header, version_purge_statuses_map,
};

#[derive(Debug, Clone, Default)]
pub struct MustReplicateOptions {
    meta: HashMap<String, String>,
    status: ReplicationStatusType,
    op_type: ReplicationType,
    replication_request: bool,
}

impl MustReplicateOptions {
    pub fn new(meta: &HashMap<String, String>, user_tags: String, op_type: ReplicationType, replication_request: bool) -> Self {
        let mut meta = meta.clone();
        if !user_tags.is_empty() {
            meta.insert(AMZ_OBJECT_TAGGING.to_string(), user_tags);
        }

        Self {
            meta,
            status: ReplicationStatusType::default(),
            op_type,
            replication_request,
        }
    }

    pub fn with_replication_status(mut self, status: ReplicationStatusType) -> Self {
        self.status = status;
        self
    }

    pub fn replication_status(&self) -> ReplicationStatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            let status = ReplicationStatusType::from(rs.as_str());
            if !status.is_empty() {
                return status;
            }
        }
        self.status.clone()
    }

    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObject
    }

    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::Metadata
    }

    pub fn is_replication_request(&self) -> bool {
        self.replication_request
    }

    pub fn user_tags(&self) -> &str {
        self.meta.get(AMZ_OBJECT_TAGGING).map(String::as_str).unwrap_or_default()
    }
}

pub fn is_ssec_encrypted(user_defined: &HashMap<String, String>) -> bool {
    user_defined.contains_key(SSEC_ALGORITHM_HEADER)
        || user_defined.contains_key(SSEC_KEY_HEADER)
        || user_defined.contains_key(SSEC_KEY_MD5_HEADER)
}

#[derive(Debug, Clone)]
pub struct ReplicationDeleteSource<'a> {
    pub user_defined: &'a HashMap<String, String>,
    pub user_tags: &'a str,
    pub delete_marker: bool,
    pub replication_status: ReplicationStatusType,
}

pub fn delete_replication_object_opts(dobj: &ObjectToDelete, source: &ReplicationDeleteSource<'_>) -> ObjectOpts {
    ObjectOpts {
        name: dobj.object_name.clone(),
        ssec: is_ssec_encrypted(source.user_defined),
        user_tags: source.user_tags.to_string(),
        delete_marker: source.delete_marker,
        version_id: dobj.version_id,
        op_type: ReplicationType::Delete,
        replica: source.replication_status == ReplicationStatusType::Replica,
        ..Default::default()
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationDeleteStateSource {
    pub name: String,
    pub user_tags: String,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub replica: bool,
}

pub fn delete_replication_state_from_config(
    config: &ReplicationConfiguration,
    source: &ReplicationDeleteStateSource,
) -> Option<ReplicationState> {
    let opts = ObjectOpts {
        name: source.name.clone(),
        user_tags: source.user_tags.clone(),
        version_id: source.version_id,
        delete_marker: source.delete_marker,
        op_type: ReplicationType::Delete,
        replica: source.replica,
        ..Default::default()
    };
    let target_arns = config.filter_target_arns(&opts);
    if target_arns.is_empty() {
        return None;
    }

    let mut decision = ReplicateDecision::new();
    for target_arn in target_arns {
        let mut target_opts = opts.clone();
        target_opts.target_arn = target_arn.clone();
        let replicate = config.replicate(&target_opts);
        decision.set(ReplicateTargetDecision::new(target_arn, replicate, false));
    }
    if !decision.replicate_any() {
        return None;
    }

    let pending_status = decision.pending_status();
    let mut state = ReplicationState {
        replicate_decision_str: decision.to_string(),
        ..Default::default()
    };
    if source.version_id.is_some() {
        state.version_purge_status_internal = pending_status.clone();
        state.purge_targets = version_purge_statuses_map(pending_status.as_deref().unwrap_or_default());
    } else {
        state.replication_status_internal = pending_status.clone();
        state.targets = replication_statuses_map(pending_status.as_deref().unwrap_or_default());
    }
    Some(state)
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicationDeleteScheduleInput<'a> {
    pub replication_request: bool,
    pub version_id_requested: bool,
    pub source_delete_marker: bool,
    pub source_replication_status: &'a ReplicationStatusType,
    pub source_version_purge_status: &'a VersionPurgeStatusType,
    pub deleted_delete_marker_version: bool,
}

fn delete_version_purge_source_status(status: &ReplicationStatusType) -> bool {
    status == &ReplicationStatusType::Replica
        || status == &ReplicationStatusType::Pending
        || status == &ReplicationStatusType::Completed
        || status == &ReplicationStatusType::Failed
}

pub fn should_schedule_delete_replication(input: ReplicationDeleteScheduleInput<'_>) -> bool {
    if input.replication_request {
        return false;
    }

    if input.version_id_requested && !input.deleted_delete_marker_version && !input.source_delete_marker {
        return delete_version_purge_source_status(input.source_replication_status);
    }

    input.source_replication_status == &ReplicationStatusType::Replica
        || input.source_replication_status == &ReplicationStatusType::Pending
        || input.source_version_purge_status == &VersionPurgeStatusType::Pending
        || (input.deleted_delete_marker_version && input.source_replication_status == &ReplicationStatusType::Completed)
}

pub fn delete_replication_version_id(
    source_delete_marker: bool,
    source_version_id: Option<Uuid>,
    deleted_delete_marker_version: bool,
) -> Option<Uuid> {
    if source_delete_marker && !deleted_delete_marker_version {
        None
    } else {
        source_version_id
    }
}

pub fn should_use_existing_delete_replication_source(
    replication_request: bool,
    deleted_delete_marker: bool,
    has_existing_source: bool,
) -> bool {
    replication_request && deleted_delete_marker && has_existing_source
}

pub fn should_use_existing_delete_replication_info(version_id_requested: bool, delete_marker_request: bool) -> bool {
    version_id_requested && !delete_marker_request
}

pub fn heal_uses_delete_replication_path(delete_marker: bool, version_purge_status: &VersionPurgeStatusType) -> bool {
    delete_marker || !version_purge_status.is_empty()
}

pub fn delete_replication_missing_source_decision(
    delete_marker: bool,
    target_status: ReplicationStatusType,
    rule_replicates: bool,
    version_purge_status: &VersionPurgeStatusType,
) -> Option<bool> {
    let valid_replication_status = matches!(
        target_status,
        ReplicationStatusType::Pending | ReplicationStatusType::Completed | ReplicationStatusType::Failed
    );

    if delete_marker && (valid_replication_status || rule_replicates) {
        return Some(rule_replicates);
    }

    if !version_purge_status.is_empty() {
        return Some(matches!(
            version_purge_status,
            &VersionPurgeStatusType::Pending | &VersionPurgeStatusType::Failed
        ));
    }

    None
}

#[derive(Debug, Clone)]
pub struct ReplicationResyncTargetObject<'a> {
    pub mod_time: Option<OffsetDateTime>,
    pub user_defined: &'a HashMap<String, String>,
}

pub fn resync_target_for_object(
    object: &ReplicationResyncTargetObject<'_>,
    arn: &str,
    reset_id: &str,
    reset_before_date: Option<OffsetDateTime>,
    status: ReplicationStatusType,
) -> ResyncTargetDecision {
    let rs = object
        .user_defined
        .get(target_reset_header(arn).as_str())
        .cloned()
        .or_else(|| get_header_map(object.user_defined, SUFFIX_REPLICATION_RESET_STATUS));

    let mut dec = ResyncTargetDecision::default();

    let mod_time = object.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);

    let Some(rs) = rs else {
        let reset_before_date = reset_before_date.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        if !reset_id.is_empty() && mod_time <= reset_before_date {
            dec.replicate = true;
            return dec;
        }

        dec.replicate = status == ReplicationStatusType::Empty;

        return dec;
    };

    let Some(reset_before_date) = reset_before_date else {
        return dec;
    };

    if reset_id.is_empty() {
        return dec;
    }

    let parts: Vec<&str> = rs.splitn(2, ';').collect();

    if parts.len() != 2 {
        return dec;
    }

    let new_reset = parts[1] != reset_id;

    if !new_reset && status == ReplicationStatusType::Completed {
        return dec;
    }

    dec.replicate = new_reset && mod_time <= reset_before_date;

    dec
}

#[cfg(test)]
mod tests {
    use super::{
        MustReplicateOptions, ReplicationDeleteScheduleInput, ReplicationDeleteSource, ReplicationDeleteStateSource,
        ReplicationResyncTargetObject, delete_replication_missing_source_decision, delete_replication_object_opts,
        delete_replication_state_from_config, delete_replication_version_id, heal_uses_delete_replication_path,
        is_ssec_encrypted, resync_target_for_object, should_schedule_delete_replication,
        should_use_existing_delete_replication_info, should_use_existing_delete_replication_source,
    };
    use crate::storage_api::ObjectToDelete;
    use crate::{ReplicationStatusType, ReplicationType, VersionPurgeStatusType, target_reset_header};
    use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, SSEC_ALGORITHM_HEADER};
    use s3s::dto::{
        DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ExistingObjectReplication,
        ExistingObjectReplicationStatus, ReplicaModifications, ReplicaModificationsStatus, ReplicationConfiguration,
        ReplicationRule, ReplicationRuleStatus, SourceSelectionCriteria,
    };
    use std::collections::HashMap;
    use time::{Duration, OffsetDateTime};
    use uuid::Uuid;

    #[test]
    fn must_replicate_options_preserves_user_tags_and_operation_type() {
        let options = MustReplicateOptions::new(&HashMap::new(), "env=prod".to_string(), ReplicationType::ExistingObject, true);

        assert_eq!(options.user_tags(), "env=prod");
        assert!(options.is_existing_object_replication());
        assert!(options.is_replication_request());
    }

    #[test]
    fn must_replicate_options_reads_replication_status_header() {
        let meta = HashMap::from([(AMZ_BUCKET_REPLICATION_STATUS.to_string(), "COMPLETED".to_string())]);
        let options = MustReplicateOptions::new(&meta, String::new(), ReplicationType::Object, false)
            .with_replication_status(ReplicationStatusType::Replica);

        assert_eq!(options.replication_status(), ReplicationStatusType::Completed);
    }

    #[test]
    fn must_replicate_options_falls_back_to_explicit_replication_status() {
        let options = MustReplicateOptions::new(&HashMap::new(), String::new(), ReplicationType::Delete, false)
            .with_replication_status(ReplicationStatusType::Replica);

        assert_eq!(options.replication_status(), ReplicationStatusType::Replica);
    }

    #[test]
    fn ssec_detection_uses_existing_metadata_headers() {
        let meta = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);

        assert!(is_ssec_encrypted(&meta));
        assert!(!is_ssec_encrypted(&HashMap::new()));
    }

    #[test]
    fn heal_delete_path_is_limited_to_delete_markers_and_version_purges() {
        assert!(heal_uses_delete_replication_path(true, &VersionPurgeStatusType::Empty));
        assert!(heal_uses_delete_replication_path(false, &VersionPurgeStatusType::Pending));
        assert!(!heal_uses_delete_replication_path(false, &VersionPurgeStatusType::Empty));
    }

    #[test]
    fn missing_source_delete_decision_keeps_delete_marker_and_purge_rules() {
        assert_eq!(
            delete_replication_missing_source_decision(
                true,
                ReplicationStatusType::Completed,
                false,
                &VersionPurgeStatusType::Empty,
            ),
            Some(false)
        );
        assert_eq!(
            delete_replication_missing_source_decision(
                false,
                ReplicationStatusType::Empty,
                false,
                &VersionPurgeStatusType::Pending,
            ),
            Some(true)
        );
        assert_eq!(
            delete_replication_missing_source_decision(
                false,
                ReplicationStatusType::Empty,
                false,
                &VersionPurgeStatusType::Empty
            ),
            None
        );
    }

    #[test]
    fn delete_replication_object_opts_marks_replica_deletes() {
        let version_id = Uuid::new_v4();
        let user_defined = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);
        let dobj = ObjectToDelete {
            object_name: "obj".to_string(),
            version_id: Some(version_id),
            ..Default::default()
        };
        let source = ReplicationDeleteSource {
            user_defined: &user_defined,
            user_tags: "env=prod",
            delete_marker: true,
            replication_status: ReplicationStatusType::Replica,
        };

        let opts = delete_replication_object_opts(&dobj, &source);

        assert!(opts.replica);
        assert!(opts.ssec);
        assert_eq!(opts.user_tags, "env=prod");
        assert_eq!(opts.version_id, Some(version_id));
        assert_eq!(opts.name, "obj");
        assert_eq!(opts.op_type, ReplicationType::Delete);
    }

    fn delete_replication_rule(arn: &str, replica_modifications: bool) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication {
                status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
            }),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
            }),
            filter: None,
            id: Some("rule-1".to_string()),
            prefix: Some("test/".to_string()),
            priority: Some(1),
            source_selection_criteria: replica_modifications.then_some(SourceSelectionCriteria {
                replica_modifications: Some(ReplicaModifications {
                    status: ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED),
                }),
                sse_kms_encrypted_objects: None,
            }),
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    #[test]
    fn delete_replication_state_tracks_downstream_delete_marker_targets() {
        let arn = "arn:aws:s3:::target-bucket";
        let config = ReplicationConfiguration {
            role: arn.to_string(),
            rules: vec![delete_replication_rule(arn, true)],
        };
        let source = ReplicationDeleteStateSource {
            name: "test/object.txt".to_string(),
            user_tags: String::new(),
            version_id: None,
            delete_marker: true,
            replica: true,
        };

        let state = delete_replication_state_from_config(&config, &source)
            .expect("replica delete marker should be forwarded to downstream targets");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.replication_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.targets.contains_key(arn));
    }

    #[test]
    fn delete_replication_state_skips_replica_delete_without_replica_modifications() {
        let arn = "arn:aws:s3:::target-bucket";
        let config = ReplicationConfiguration {
            role: arn.to_string(),
            rules: vec![delete_replication_rule(arn, false)],
        };
        let source = ReplicationDeleteStateSource {
            name: "test/object.txt".to_string(),
            user_tags: String::new(),
            version_id: None,
            delete_marker: true,
            replica: true,
        };

        assert!(delete_replication_state_from_config(&config, &source).is_none());
    }

    #[test]
    fn delete_replication_state_tracks_delete_marker_version_purges() {
        let arn = "arn:aws:s3:::target-bucket";
        let config = ReplicationConfiguration {
            role: arn.to_string(),
            rules: vec![delete_replication_rule(arn, false)],
        };
        let source = ReplicationDeleteStateSource {
            name: "test/object.txt".to_string(),
            user_tags: String::new(),
            version_id: Some(Uuid::new_v4()),
            delete_marker: true,
            replica: false,
        };

        let state = delete_replication_state_from_config(&config, &source)
            .expect("delete-marker version purge should honor delete-marker replication rules");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.version_purge_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.purge_targets.contains_key(arn));
    }

    #[test]
    fn delete_replication_schedule_skips_replica_requests() {
        assert!(!should_schedule_delete_replication(ReplicationDeleteScheduleInput {
            replication_request: true,
            version_id_requested: true,
            source_delete_marker: true,
            source_replication_status: &ReplicationStatusType::Completed,
            source_version_purge_status: &VersionPurgeStatusType::Empty,
            deleted_delete_marker_version: true,
        }));
    }

    #[test]
    fn delete_replication_schedule_keeps_marker_and_version_purges() {
        assert!(should_schedule_delete_replication(ReplicationDeleteScheduleInput {
            replication_request: false,
            version_id_requested: true,
            source_delete_marker: true,
            source_replication_status: &ReplicationStatusType::Completed,
            source_version_purge_status: &VersionPurgeStatusType::Empty,
            deleted_delete_marker_version: true,
        }));
        assert!(should_schedule_delete_replication(ReplicationDeleteScheduleInput {
            replication_request: false,
            version_id_requested: true,
            source_delete_marker: false,
            source_replication_status: &ReplicationStatusType::Completed,
            source_version_purge_status: &VersionPurgeStatusType::Empty,
            deleted_delete_marker_version: false,
        }));
        assert!(should_schedule_delete_replication(ReplicationDeleteScheduleInput {
            replication_request: false,
            version_id_requested: false,
            source_delete_marker: false,
            source_replication_status: &ReplicationStatusType::Empty,
            source_version_purge_status: &VersionPurgeStatusType::Pending,
            deleted_delete_marker_version: false,
        }));
    }

    #[test]
    fn delete_replication_version_id_splits_marker_creation_and_purge() {
        let version_id = Uuid::new_v4();

        assert_eq!(delete_replication_version_id(true, Some(version_id), false), None);
        assert_eq!(delete_replication_version_id(true, Some(version_id), true), Some(version_id));
    }

    #[test]
    fn delete_replication_source_selection_prefers_existing_marker_source_only_for_replica_requests() {
        assert!(should_use_existing_delete_replication_source(true, true, true));
        assert!(!should_use_existing_delete_replication_source(false, true, true));
        assert!(!should_use_existing_delete_replication_source(true, false, true));
        assert!(!should_use_existing_delete_replication_source(true, true, false));
    }

    #[test]
    fn existing_delete_replication_info_is_limited_to_version_delete_requests() {
        assert!(should_use_existing_delete_replication_info(true, false));
        assert!(!should_use_existing_delete_replication_info(true, true));
        assert!(!should_use_existing_delete_replication_info(false, false));
    }

    #[test]
    fn resync_target_includes_object_at_reset_before_boundary() {
        let reset_before = OffsetDateTime::UNIX_EPOCH + Duration::seconds(30);
        let user_defined = HashMap::new();
        let object = ReplicationResyncTargetObject {
            mod_time: Some(reset_before),
            user_defined: &user_defined,
        };

        let decision =
            resync_target_for_object(&object, "arn:target", "reset-1", Some(reset_before), ReplicationStatusType::Completed);

        assert!(decision.replicate);
    }

    #[test]
    fn resync_target_replicates_when_reset_id_changes() {
        let reset_before = OffsetDateTime::UNIX_EPOCH + Duration::seconds(30);
        let user_defined = HashMap::from([(target_reset_header("arn:target"), "1970-01-01T00:00:20Z;old-reset".to_string())]);
        let object = ReplicationResyncTargetObject {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            user_defined: &user_defined,
        };

        let decision =
            resync_target_for_object(&object, "arn:target", "new-reset", Some(reset_before), ReplicationStatusType::Completed);

        assert!(decision.replicate);
    }

    #[test]
    fn resync_target_skips_completed_object_for_same_reset_id() {
        let reset_before = OffsetDateTime::UNIX_EPOCH + Duration::seconds(30);
        let user_defined = HashMap::from([(target_reset_header("arn:target"), "1970-01-01T00:00:20Z;same-reset".to_string())]);
        let object = ReplicationResyncTargetObject {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            user_defined: &user_defined,
        };

        let decision =
            resync_target_for_object(&object, "arn:target", "same-reset", Some(reset_before), ReplicationStatusType::Completed);

        assert!(!decision.replicate);
    }
}
