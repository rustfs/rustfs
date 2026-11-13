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

//! The s3s edge of the bucket replication configuration.
//!
//! Responsible for: the one conversion between the s3s DTO the legacy S3 stack
//! decodes (`PutBucketReplication`) or encodes (`GetBucketReplication`) and the
//! gateway persistence shape the replication engine, the bucket metadata and
//! site replication work on (`rustfs_gateway_types::persistence`).
//!
//! Not responsible for: validating a configuration (`rustfs-replication`) or
//! storing it (`rustfs-ecstore`).
//!
//! Both directions are total and lossless: every s3s member has a persisted
//! member of the same optionality, and every s3s string enum carries its wire
//! text, so an unknown status or storage class crosses unchanged. The one s3s
//! member without a counterpart, `ReplicationRuleFilter::cached_tags`, is a
//! parse cache with no wire form.
//!
//! Upstream: `storage::ecfs` (`put_bucket_replication`,
//! `get_bucket_replication`). Downstream: `app::bucket_usecase`, which takes and
//! answers the persistence shape. The gateway HTTP stack retires this edge
//! (T1.8, rustfs/backlog#2749).

use rustfs_gateway_types::persistence::{
    PersistedAccessControlTranslation, PersistedEncryptionConfiguration, PersistedOptionalReplicationStatus,
    PersistedReplicationAnd, PersistedReplicationConfiguration, PersistedReplicationDestination, PersistedReplicationFilter,
    PersistedReplicationMetrics, PersistedReplicationRule, PersistedReplicationStatus, PersistedReplicationTag,
    PersistedReplicationTime, PersistedReplicationTimeValue, PersistedSourceSelectionCriteria,
};
use s3s::dto;

/// The decoded `PutBucketReplication` body in the shape the engine stores.
pub(crate) fn replication_configuration_from_s3s(value: dto::ReplicationConfiguration) -> PersistedReplicationConfiguration {
    PersistedReplicationConfiguration {
        role: value.role,
        rules: value.rules.into_iter().map(rule_from_s3s).collect(),
    }
}

/// A stored configuration in the shape the legacy `GetBucketReplication` encoder takes.
pub(crate) fn replication_configuration_to_s3s(value: PersistedReplicationConfiguration) -> dto::ReplicationConfiguration {
    dto::ReplicationConfiguration {
        role: value.role,
        rules: value.rules.into_iter().map(rule_to_s3s).collect(),
    }
}

/// s3s string enums become their wire text; the persisted shape keeps every
/// value, known or not.
fn status_text(value: &str) -> String {
    value.to_owned()
}

fn rule_from_s3s(value: dto::ReplicationRule) -> PersistedReplicationRule {
    PersistedReplicationRule {
        delete_marker_replication: value
            .delete_marker_replication
            .map(|wrapper| PersistedOptionalReplicationStatus {
                status: wrapper.status.map(|status| status_text(status.as_str())),
            }),
        delete_replication: value.delete_replication.map(|wrapper| PersistedReplicationStatus {
            status: status_text(wrapper.status.as_str()),
        }),
        destination: destination_from_s3s(value.destination),
        existing_object_replication: value.existing_object_replication.map(|wrapper| PersistedReplicationStatus {
            status: status_text(wrapper.status.as_str()),
        }),
        filter: value.filter.map(filter_from_s3s),
        id: value.id,
        prefix: value.prefix,
        priority: value.priority,
        source_selection_criteria: value
            .source_selection_criteria
            .map(|criteria| PersistedSourceSelectionCriteria {
                replica_modifications: criteria.replica_modifications.map(|wrapper| PersistedReplicationStatus {
                    status: status_text(wrapper.status.as_str()),
                }),
                sse_kms_encrypted_objects: criteria.sse_kms_encrypted_objects.map(|wrapper| PersistedReplicationStatus {
                    status: status_text(wrapper.status.as_str()),
                }),
            }),
        status: status_text(value.status.as_str()),
    }
}

fn destination_from_s3s(value: dto::Destination) -> PersistedReplicationDestination {
    PersistedReplicationDestination {
        account: value.account,
        access_control_translation: value
            .access_control_translation
            .map(|translation| PersistedAccessControlTranslation {
                owner: status_text(translation.owner.as_str()),
            }),
        bucket: value.bucket,
        encryption_configuration: value
            .encryption_configuration
            .map(|configuration| PersistedEncryptionConfiguration {
                replica_kms_key_id: configuration.replica_kms_key_id,
            }),
        metrics: value.metrics.map(|metrics| PersistedReplicationMetrics {
            event_threshold: metrics.event_threshold.map(time_value_from_s3s),
            status: status_text(metrics.status.as_str()),
        }),
        replication_time: value.replication_time.map(|replication_time| PersistedReplicationTime {
            status: status_text(replication_time.status.as_str()),
            time: time_value_from_s3s(replication_time.time),
        }),
        storage_class: value.storage_class.map(|class| status_text(class.as_str())),
    }
}

fn time_value_from_s3s(value: dto::ReplicationTimeValue) -> PersistedReplicationTimeValue {
    PersistedReplicationTimeValue { minutes: value.minutes }
}

fn filter_from_s3s(value: dto::ReplicationRuleFilter) -> PersistedReplicationFilter {
    PersistedReplicationFilter {
        and: value.and.map(|and| PersistedReplicationAnd {
            prefix: and.prefix,
            tags: and.tags.map(|tags| tags.into_iter().map(tag_from_s3s).collect()),
        }),
        prefix: value.prefix,
        tag: value.tag.map(tag_from_s3s),
    }
}

/// Both members stay optional: a keyless `<Tag/>` (which the console's rule
/// form sends) is a filter the engine reads as absent, not a malformed body.
fn tag_from_s3s(value: dto::Tag) -> PersistedReplicationTag {
    PersistedReplicationTag {
        key: value.key,
        value: value.value,
    }
}

fn rule_to_s3s(value: PersistedReplicationRule) -> dto::ReplicationRule {
    dto::ReplicationRule {
        delete_marker_replication: value.delete_marker_replication.map(|wrapper| dto::DeleteMarkerReplication {
            status: wrapper.status.map(dto::DeleteMarkerReplicationStatus::from),
        }),
        delete_replication: value.delete_replication.map(|wrapper| dto::DeleteReplication {
            status: dto::DeleteReplicationStatus::from(wrapper.status),
        }),
        destination: destination_to_s3s(value.destination),
        existing_object_replication: value
            .existing_object_replication
            .map(|wrapper| dto::ExistingObjectReplication {
                status: dto::ExistingObjectReplicationStatus::from(wrapper.status),
            }),
        filter: value.filter.map(filter_to_s3s),
        id: value.id,
        prefix: value.prefix,
        priority: value.priority,
        source_selection_criteria: value.source_selection_criteria.map(|criteria| dto::SourceSelectionCriteria {
            replica_modifications: criteria.replica_modifications.map(|wrapper| dto::ReplicaModifications {
                status: dto::ReplicaModificationsStatus::from(wrapper.status),
            }),
            sse_kms_encrypted_objects: criteria.sse_kms_encrypted_objects.map(|wrapper| dto::SseKmsEncryptedObjects {
                status: dto::SseKmsEncryptedObjectsStatus::from(wrapper.status),
            }),
        }),
        status: dto::ReplicationRuleStatus::from(value.status),
    }
}

fn destination_to_s3s(value: PersistedReplicationDestination) -> dto::Destination {
    dto::Destination {
        access_control_translation: value
            .access_control_translation
            .map(|translation| dto::AccessControlTranslation {
                owner: dto::OwnerOverride::from(translation.owner),
            }),
        account: value.account,
        bucket: value.bucket,
        encryption_configuration: value
            .encryption_configuration
            .map(|configuration| dto::EncryptionConfiguration {
                replica_kms_key_id: configuration.replica_kms_key_id,
            }),
        metrics: value.metrics.map(|metrics| dto::Metrics {
            event_threshold: metrics.event_threshold.map(time_value_to_s3s),
            status: dto::MetricsStatus::from(metrics.status),
        }),
        replication_time: value.replication_time.map(|replication_time| dto::ReplicationTime {
            status: dto::ReplicationTimeStatus::from(replication_time.status),
            time: time_value_to_s3s(replication_time.time),
        }),
        storage_class: value.storage_class.map(dto::StorageClass::from),
    }
}

fn time_value_to_s3s(value: PersistedReplicationTimeValue) -> dto::ReplicationTimeValue {
    dto::ReplicationTimeValue { minutes: value.minutes }
}

fn filter_to_s3s(value: PersistedReplicationFilter) -> dto::ReplicationRuleFilter {
    dto::ReplicationRuleFilter {
        and: value.and.map(|and| dto::ReplicationRuleAndOperator {
            prefix: and.prefix,
            tags: and.tags.map(|tags| tags.into_iter().map(tag_to_s3s).collect()),
        }),
        prefix: value.prefix,
        tag: value.tag.map(tag_to_s3s),
        ..Default::default()
    }
}

fn tag_to_s3s(value: PersistedReplicationTag) -> dto::Tag {
    dto::Tag {
        key: value.key,
        value: value.value,
    }
}

#[cfg(test)]
mod tests {
    use super::{replication_configuration_from_s3s, replication_configuration_to_s3s};
    use rustfs_gateway_types::persistence::{PersistedReplicationConfiguration, parse_replication, serialize_replication};
    use s3s::{dto, xml};

    fn s3s_bytes(value: &dto::ReplicationConfiguration) -> Vec<u8> {
        let mut buf = Vec::new();
        xml::Serialize::serialize(value, &mut xml::Serializer::new(&mut buf)).expect("the s3s encoder writes the fixture");
        buf
    }

    fn tag(key: &str, value: &str) -> dto::Tag {
        dto::Tag {
            key: Some(key.to_owned()),
            value: Some(value.to_owned()),
        }
    }

    /// Every member of the family populated, each string enum carrying a value
    /// outside its known set, so a conversion that drops or normalizes one
    /// member cannot pass.
    fn every_member() -> dto::ReplicationConfiguration {
        dto::ReplicationConfiguration {
            role: "arn:aws:iam::123456789012:role/replication".to_owned(),
            rules: vec![
                dto::ReplicationRule {
                    delete_marker_replication: Some(dto::DeleteMarkerReplication {
                        status: Some(dto::DeleteMarkerReplicationStatus::from("FutureDeleteMarker".to_owned())),
                    }),
                    delete_replication: Some(dto::DeleteReplication {
                        status: dto::DeleteReplicationStatus::from("FutureDelete".to_owned()),
                    }),
                    destination: dto::Destination {
                        access_control_translation: Some(dto::AccessControlTranslation {
                            owner: dto::OwnerOverride::from("FutureOwner".to_owned()),
                        }),
                        account: Some("123456789012".to_owned()),
                        bucket: "arn:aws:s3:::backup".to_owned(),
                        encryption_configuration: Some(dto::EncryptionConfiguration {
                            replica_kms_key_id: Some("kms-key".to_owned()),
                        }),
                        metrics: Some(dto::Metrics {
                            event_threshold: Some(dto::ReplicationTimeValue { minutes: Some(15) }),
                            status: dto::MetricsStatus::from("FutureMetrics".to_owned()),
                        }),
                        replication_time: Some(dto::ReplicationTime {
                            status: dto::ReplicationTimeStatus::from("FutureRtc".to_owned()),
                            time: dto::ReplicationTimeValue { minutes: Some(30) },
                        }),
                        storage_class: Some(dto::StorageClass::from("FutureClass".to_owned())),
                    },
                    existing_object_replication: Some(dto::ExistingObjectReplication {
                        status: dto::ExistingObjectReplicationStatus::from("FutureExisting".to_owned()),
                    }),
                    filter: Some(dto::ReplicationRuleFilter {
                        and: Some(dto::ReplicationRuleAndOperator {
                            prefix: Some("docs/".to_owned()),
                            tags: Some(vec![tag("team", "storage"), tag("tier", "gold")]),
                        }),
                        prefix: Some("filter/".to_owned()),
                        tag: Some(tag("env", "prod")),
                        ..Default::default()
                    }),
                    id: Some("every-member".to_owned()),
                    prefix: Some("legacy/".to_owned()),
                    priority: Some(7),
                    source_selection_criteria: Some(dto::SourceSelectionCriteria {
                        replica_modifications: Some(dto::ReplicaModifications {
                            status: dto::ReplicaModificationsStatus::from("FutureReplica".to_owned()),
                        }),
                        sse_kms_encrypted_objects: Some(dto::SseKmsEncryptedObjects {
                            status: dto::SseKmsEncryptedObjectsStatus::from("FutureKms".to_owned()),
                        }),
                    }),
                    status: dto::ReplicationRuleStatus::from("FutureRule".to_owned()),
                },
                rule("arn:aws:s3:::second", dto::ReplicationRuleStatus::DISABLED),
            ],
        }
    }

    /// A rule with only its required members.
    fn rule(bucket: &str, status: &'static str) -> dto::ReplicationRule {
        dto::ReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: dto::Destination {
                bucket: bucket.to_owned(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: None,
            prefix: None,
            priority: None,
            source_selection_criteria: None,
            status: dto::ReplicationRuleStatus::from_static(status),
        }
    }

    fn one_filter(filter: dto::ReplicationRuleFilter) -> dto::ReplicationConfiguration {
        let mut rule = rule("arn:aws:s3:::backup", dto::ReplicationRuleStatus::ENABLED);
        rule.filter = Some(filter);
        dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        }
    }

    #[test]
    fn every_member_crosses_to_the_engine_and_back_unchanged() {
        let crossed = replication_configuration_to_s3s(replication_configuration_from_s3s(every_member()));
        assert_eq!(crossed, every_member());
    }

    #[test]
    fn the_engine_shape_persists_the_bytes_the_s3s_encoder_wrote() {
        let persisted = serialize_replication(&replication_configuration_from_s3s(every_member()))
            .expect("the gateway writer accepts the converted configuration");
        assert_eq!(String::from_utf8_lossy(&persisted), String::from_utf8_lossy(&s3s_bytes(&every_member())));
    }

    #[test]
    fn stored_bytes_answer_the_s3s_get_unchanged() {
        let stored = parse_replication(&s3s_bytes(&every_member())).expect("the gateway parser reads what s3s wrote");
        assert_eq!(replication_configuration_to_s3s(stored), every_member());
    }

    #[test]
    fn a_keyless_tag_stays_keyless_instead_of_becoming_an_empty_key() {
        let config = replication_configuration_from_s3s(one_filter(dto::ReplicationRuleFilter {
            tag: Some(dto::Tag { key: None, value: None }),
            ..Default::default()
        }));
        let tag = config.rules[0]
            .filter
            .as_ref()
            .and_then(|filter| filter.tag.as_ref())
            .expect("the keyless tag is kept, not dropped");
        assert_eq!(tag.key, None);
        assert_eq!(tag.value, None);
    }

    #[test]
    fn an_empty_tag_key_stays_an_empty_key_instead_of_becoming_absent() {
        let config = replication_configuration_from_s3s(one_filter(dto::ReplicationRuleFilter {
            tag: Some(tag("", "")),
            ..Default::default()
        }));
        let tag = config.rules[0].filter.as_ref().and_then(|filter| filter.tag.as_ref());
        assert_eq!(tag.and_then(|tag| tag.key.as_deref()), Some(""));
        assert_eq!(tag.and_then(|tag| tag.value.as_deref()), Some(""));
    }

    #[test]
    fn an_empty_and_tag_list_stays_distinct_from_an_absent_one() {
        let empty = replication_configuration_from_s3s(one_filter(dto::ReplicationRuleFilter {
            and: Some(dto::ReplicationRuleAndOperator {
                prefix: None,
                tags: Some(Vec::new()),
            }),
            ..Default::default()
        }));
        let absent = replication_configuration_from_s3s(one_filter(dto::ReplicationRuleFilter {
            and: Some(dto::ReplicationRuleAndOperator {
                prefix: None,
                tags: None,
            }),
            ..Default::default()
        }));
        let tags = |config: &PersistedReplicationConfiguration| {
            config.rules[0]
                .filter
                .as_ref()
                .and_then(|filter| filter.and.as_ref())
                .map(|and| and.tags.clone())
        };
        assert_eq!(tags(&empty), Some(Some(Vec::new())));
        assert_eq!(tags(&absent), Some(None));
    }

    #[test]
    fn an_unknown_status_keeps_its_text_so_the_capability_gate_still_sees_it() {
        let mut source = one_filter(dto::ReplicationRuleFilter::default());
        source.rules[0].status = dto::ReplicationRuleStatus::from("enabled".to_owned());
        let config = replication_configuration_from_s3s(source);
        assert_eq!(
            config.rules[0].status, "enabled",
            "a lowercase status must reach the capability gate as sent, not normalized into a valid one"
        );
        let back = replication_configuration_to_s3s(config);
        assert_eq!(back.rules[0].status.as_str(), "enabled");
    }

    #[test]
    fn a_delete_marker_wrapper_without_status_stays_without_status() {
        let mut source = one_filter(dto::ReplicationRuleFilter::default());
        source.rules[0].delete_marker_replication = Some(dto::DeleteMarkerReplication { status: None });
        let config = replication_configuration_from_s3s(source);
        let wrapper = config.rules[0]
            .delete_marker_replication
            .as_ref()
            .expect("the empty wrapper is kept");
        assert_eq!(wrapper.status, None);
        let back = replication_configuration_to_s3s(config);
        assert_eq!(
            back.rules[0].delete_marker_replication,
            Some(dto::DeleteMarkerReplication { status: None })
        );
    }

    #[test]
    fn an_absent_storage_class_is_not_defaulted() {
        let config = replication_configuration_from_s3s(one_filter(dto::ReplicationRuleFilter::default()));
        assert_eq!(config.rules[0].destination.storage_class, None);
        let back = replication_configuration_to_s3s(config);
        assert_eq!(back.rules[0].destination.storage_class, None);
    }

    fn s3s_reads(input: &[u8]) -> bool {
        let mut deserializer = xml::Deserializer::new(input);
        <dto::ReplicationConfiguration as xml::Deserialize>::deserialize(&mut deserializer).is_ok()
            && deserializer.expect_eof().is_ok()
    }

    /// The persisted reader the engine now uses is no more permissive than the
    /// s3s reader it replaces: every document s3s refused stays refused.
    #[test]
    fn the_gateway_reader_refuses_what_the_s3s_reader_refused() {
        let refused: [&[u8]; 7] = [
            b"<ReplicationConfiguration><Role></Role><Rule><Future>x</Future><Status>Enabled</Status><Destination><Bucket>b</Bucket></Destination></Rule></ReplicationConfiguration>",
            b"<ReplicationConfiguration><Role></Role><Rule><Status>Enabled</Status><Destination><Bucket>b</Bucket><Future>x</Future></Destination></Rule></ReplicationConfiguration>",
            b"<ReplicationConfiguration><Role></Role><Rule><Status>Enabled</Status><Filter><Tag><Key>k</Key><Future>x</Future></Tag></Filter><Destination><Bucket>b</Bucket></Destination></Rule></ReplicationConfiguration>",
            b"<ReplicationConfiguration><Role></Role><Rule><Status>Enabled</Status><Priority>high</Priority><Destination><Bucket>b</Bucket></Destination></Rule></ReplicationConfiguration>",
            b"<ReplicationConfiguration><Role></Role><Rule><Destination><Bucket>b</Bucket></Destination></Rule></ReplicationConfiguration>",
            b"<ReplicationConfiguration><Role></Role><Rule><Status>Enabled</Status></Rule></ReplicationConfiguration>",
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
        ];
        for input in refused {
            let text = String::from_utf8_lossy(input);
            assert!(!s3s_reads(input), "fixture is not a document s3s refuses: {text}");
            assert!(parse_replication(input).is_err(), "the gateway reader accepted what s3s refused: {text}");
        }
    }

    /// The other direction: a root-level element neither reader knows is
    /// skipped by both, so the gateway reader is no stricter either.
    #[test]
    fn both_readers_skip_an_unknown_root_level_element() {
        let input: &[u8] = b"<ReplicationConfiguration><Role></Role><FutureTopLevel>x</FutureTopLevel><Rule><Status>Enabled</Status><Destination><Bucket>b</Bucket></Destination></Rule></ReplicationConfiguration>";
        assert!(s3s_reads(input));
        let stored = parse_replication(input).expect("the gateway reader skips the unknown root element");
        assert_eq!(stored.rules.len(), 1);
    }
}
