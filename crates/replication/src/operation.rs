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

use rustfs_storage_api::ObjectToDelete;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER,
    SUFFIX_REPLICATION_RESET_STATUS, get_header_map,
};
use time::OffsetDateTime;

use crate::{
    ObjectOpts, ReplicationStatusType, ReplicationType, ResyncTargetDecision, VersionPurgeStatusType, target_reset_header,
};

#[derive(Debug, Clone, Default)]
pub struct MustReplicateOptions {
    meta: HashMap<String, String>,
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
            op_type,
            replication_request,
        }
    }

    pub fn replication_status(&self) -> ReplicationStatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            return ReplicationStatusType::from(rs.as_str());
        }
        ReplicationStatusType::default()
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

pub fn heal_uses_delete_replication_path(delete_marker: bool, version_purge_status: VersionPurgeStatusType) -> bool {
    delete_marker || !version_purge_status.is_empty()
}

pub fn delete_replication_missing_source_decision(
    delete_marker: bool,
    target_status: ReplicationStatusType,
    rule_replicates: bool,
    version_purge_status: VersionPurgeStatusType,
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
            VersionPurgeStatusType::Pending | VersionPurgeStatusType::Failed
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
        MustReplicateOptions, ReplicationDeleteSource, ReplicationResyncTargetObject, delete_replication_missing_source_decision,
        delete_replication_object_opts, heal_uses_delete_replication_path, is_ssec_encrypted, resync_target_for_object,
    };
    use crate::{ReplicationStatusType, ReplicationType, VersionPurgeStatusType, target_reset_header};
    use rustfs_storage_api::ObjectToDelete;
    use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, SSEC_ALGORITHM_HEADER};
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
        let options = MustReplicateOptions::new(&meta, String::new(), ReplicationType::Object, false);

        assert_eq!(options.replication_status(), ReplicationStatusType::Completed);
    }

    #[test]
    fn ssec_detection_uses_existing_metadata_headers() {
        let meta = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);

        assert!(is_ssec_encrypted(&meta));
        assert!(!is_ssec_encrypted(&HashMap::new()));
    }

    #[test]
    fn heal_delete_path_is_limited_to_delete_markers_and_version_purges() {
        assert!(heal_uses_delete_replication_path(true, VersionPurgeStatusType::Empty));
        assert!(heal_uses_delete_replication_path(false, VersionPurgeStatusType::Pending));
        assert!(!heal_uses_delete_replication_path(false, VersionPurgeStatusType::Empty));
    }

    #[test]
    fn missing_source_delete_decision_keeps_delete_marker_and_purge_rules() {
        assert_eq!(
            delete_replication_missing_source_decision(
                true,
                ReplicationStatusType::Completed,
                false,
                VersionPurgeStatusType::Empty,
            ),
            Some(false)
        );
        assert_eq!(
            delete_replication_missing_source_decision(
                false,
                ReplicationStatusType::Empty,
                false,
                VersionPurgeStatusType::Pending,
            ),
            Some(true)
        );
        assert_eq!(
            delete_replication_missing_source_decision(false, ReplicationStatusType::Empty, false, VersionPurgeStatusType::Empty),
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
