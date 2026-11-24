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

//! Replication fixtures written by the historical S3 codec at 5761ddfe.
//! The XML lives in bucket metadata. The scanner keeps compact MessagePack.

use super::storage_api::ecstore_bucket::{replication::ReplicationConfig, utils::BucketConfigXml};
use rustfs_gateway_types::persistence::{
    PersistedAccessControlTranslation, PersistedEncryptionConfiguration, PersistedOptionalReplicationStatus,
    PersistedReplicationAnd, PersistedReplicationConfiguration, PersistedReplicationDestination, PersistedReplicationFilter,
    PersistedReplicationMetrics, PersistedReplicationRule, PersistedReplicationStatus, PersistedReplicationTag,
    PersistedReplicationTime, PersistedReplicationTimeValue, PersistedSourceSelectionCriteria,
};

const ALL_XML: &[u8] = b"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replication</Role><Rule>\
<DeleteMarkerReplication><Status>FutureDeleteMarker</Status></DeleteMarkerReplication>\
<DeleteReplication><Status>FutureDelete</Status></DeleteReplication><Destination>\
<AccessControlTranslation><Owner>FutureOwner</Owner></AccessControlTranslation>\
<Account>123456789012</Account><Bucket>arn:aws:s3:::backup</Bucket><EncryptionConfiguration>\
<ReplicaKmsKeyID>kms-key</ReplicaKmsKeyID></EncryptionConfiguration><Metrics><EventThreshold>\
<Minutes>15</Minutes></EventThreshold><Status>FutureMetrics</Status></Metrics><ReplicationTime>\
<Status>FutureRtc</Status><Time><Minutes>30</Minutes></Time></ReplicationTime>\
<StorageClass>FutureClass</StorageClass></Destination><ExistingObjectReplication>\
<Status>FutureExisting</Status></ExistingObjectReplication><Filter><And><Prefix>docs/</Prefix><Tag>\
<Key>team</Key><Value>storage</Value></Tag><Tag><Key>tier</Key><Value>gold</Value></Tag></And>\
<Prefix>filter/</Prefix><Tag><Key>env</Key><Value>prod</Value></Tag></Filter><ID>every-member</ID>\
<Prefix>legacy/</Prefix><Priority>7</Priority><SourceSelectionCriteria><ReplicaModifications>\
<Status>FutureReplica</Status></ReplicaModifications><SseKmsEncryptedObjects><Status>FutureKms</Status>\
</SseKmsEncryptedObjects></SourceSelectionCriteria><Status>FutureRule</Status></Rule><Rule>\
<Destination><Bucket>arn:aws:s3:::second</Bucket></Destination><Status>Disabled</Status></Rule>\
</ReplicationConfiguration>";

const KEYLESS_XML: &[u8] = b"<ReplicationConfiguration><Role></Role><Rule><Destination><Bucket>arn:aws:s3:::backup</Bucket>\
</Destination><Filter><Tag></Tag></Filter><Status>Enabled</Status></Rule></ReplicationConfiguration>";

const EMPTY_KEY_XML: &[u8] = b"<ReplicationConfiguration><Role></Role><Rule><Destination><Bucket>arn:aws:s3:::backup</Bucket>\
</Destination><Filter><Tag><Key></Key><Value></Value></Tag></Filter><Status>Enabled</Status></Rule>\
</ReplicationConfiguration>";

const LOWERCASE_XML: &[u8] = b"<ReplicationConfiguration><Role></Role><Rule><Destination><Bucket>arn:aws:s3:::backup</Bucket>\
</Destination><Filter></Filter><Status>enabled</Status></Rule></ReplicationConfiguration>";

const MARKER_XML: &[u8] = b"<ReplicationConfiguration><Role></Role><Rule><DeleteMarkerReplication></DeleteMarkerReplication>\
<Destination><Bucket>arn:aws:s3:::backup</Bucket></Destination><Filter></Filter>\
<Status>Enabled</Status></Rule></ReplicationConfiguration>";

const MINIMAL_XML: &[u8] = b"<ReplicationConfiguration><Role></Role><Rule><Destination><Bucket>arn:aws:s3:::backup</Bucket>\
</Destination><Filter></Filter><Status>Enabled</Status></Rule></ReplicationConfiguration>";

const ALL_CACHE: &str = "9292d92a61726e3a6177733a69616d3a3a3132333435363738393031323a726f6c652f7265706c69636174696f6e929a\
91b246757475726544656c6574654d61726b657291ac46757475726544656c6574659791ab4675747572654f776e6572\
ac313233343536373839303132b361726e3a6177733a73333a3a3a6261636b757091a76b6d732d6b657992910fad4675\
747572654d65747269637392a9467574757265527463911eab467574757265436c61737391ae46757475726545786973\
74696e679492a5646f63732f9292a47465616da773746f7261676592a474696572a4676f6c6480a766696c7465722f92\
a3656e76a470726f64ac65766572792d6d656d626572a76c65676163792f079291ad4675747572655265706c69636191\
a94675747572654b6d73aa46757475726552756c659ac0c097c0c0b361726e3a6177733a73333a3a3a7365636f6e64c0\
c0c0c0c0c0c0c0c0c0a844697361626c6564c0";

const EMPTY_TAGS_CACHE: &str = "9292a0919ac0c097c0c0b361726e3a6177733a73333a3a3a6261636b7570c0c0c0c0c09492c09080c0c0c0c0c0c0a745\
6e61626c6564c0";

const ABSENT_TAGS_CACHE: &str = "9292a0919ac0c097c0c0b361726e3a6177733a73333a3a3a6261636b7570c0c0c0c0c09492c0c080c0c0c0c0c0c0a745\
6e61626c6564c0";

fn status(value: &str) -> PersistedReplicationStatus {
    PersistedReplicationStatus {
        status: value.to_owned(),
    }
}

fn tag(key: &str, value: &str) -> PersistedReplicationTag {
    PersistedReplicationTag {
        key: Some(key.to_owned()),
        value: Some(value.to_owned()),
    }
}

fn every_member() -> PersistedReplicationConfiguration {
    PersistedReplicationConfiguration {
        role: "arn:aws:iam::123456789012:role/replication".to_owned(),
        rules: vec![
            PersistedReplicationRule {
                delete_marker_replication: Some(PersistedOptionalReplicationStatus {
                    status: Some("FutureDeleteMarker".to_owned()),
                }),
                delete_replication: Some(status("FutureDelete")),
                destination: PersistedReplicationDestination {
                    account: Some("123456789012".to_owned()),
                    access_control_translation: Some(PersistedAccessControlTranslation {
                        owner: "FutureOwner".to_owned(),
                    }),
                    bucket: "arn:aws:s3:::backup".to_owned(),
                    encryption_configuration: Some(PersistedEncryptionConfiguration {
                        replica_kms_key_id: Some("kms-key".to_owned()),
                    }),
                    metrics: Some(PersistedReplicationMetrics {
                        event_threshold: Some(PersistedReplicationTimeValue { minutes: Some(15) }),
                        status: "FutureMetrics".to_owned(),
                    }),
                    replication_time: Some(PersistedReplicationTime {
                        status: "FutureRtc".to_owned(),
                        time: PersistedReplicationTimeValue { minutes: Some(30) },
                    }),
                    storage_class: Some("FutureClass".to_owned()),
                },
                existing_object_replication: Some(status("FutureExisting")),
                filter: Some(PersistedReplicationFilter {
                    and: Some(PersistedReplicationAnd {
                        prefix: Some("docs/".to_owned()),
                        tags: Some(vec![tag("team", "storage"), tag("tier", "gold")]),
                    }),
                    prefix: Some("filter/".to_owned()),
                    tag: Some(tag("env", "prod")),
                }),
                id: Some("every-member".to_owned()),
                prefix: Some("legacy/".to_owned()),
                priority: Some(7),
                source_selection_criteria: Some(PersistedSourceSelectionCriteria {
                    replica_modifications: Some(status("FutureReplica")),
                    sse_kms_encrypted_objects: Some(status("FutureKms")),
                }),
                status: "FutureRule".to_owned(),
            },
            PersistedReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: PersistedReplicationDestination {
                    bucket: "arn:aws:s3:::second".to_owned(),
                    ..Default::default()
                },
                existing_object_replication: None,
                filter: None,
                id: None,
                prefix: None,
                priority: None,
                source_selection_criteria: None,
                status: "Disabled".to_owned(),
            },
        ],
    }
}

fn read_xml(input: &[u8]) -> PersistedReplicationConfiguration {
    PersistedReplicationConfiguration::decode_config_xml(input).unwrap()
}

fn read_cache(hex: &str) -> PersistedReplicationConfiguration {
    let bytes = hex_simd::decode_to_vec(hex).unwrap();
    let cache: ReplicationConfig = rmp_serde::from_slice(&bytes).unwrap();
    assert!(cache.remotes.is_none());
    assert_eq!(rmp_serde::to_vec(&cache).unwrap(), bytes);
    cache.config.unwrap()
}

#[test]
fn full_scanner_cache_keeps_every_member() {
    let config = every_member();
    assert_eq!(read_cache(ALL_CACHE), config);
    let cache = ReplicationConfig::new(Some(config), None);
    assert_eq!(rmp_serde::to_vec(&cache).unwrap(), hex_simd::decode_to_vec(ALL_CACHE).unwrap());
}

#[test]
fn bucket_metadata_writes_the_historical_xml_bytes() {
    assert_eq!(every_member().encode_config_xml().unwrap(), ALL_XML);
}

#[test]
fn bucket_metadata_reads_every_member_of_the_historical_xml() {
    assert_eq!(read_xml(ALL_XML), every_member());
}

#[test]
fn a_tag_without_key_or_value_survives() {
    let config = read_xml(KEYLESS_XML);
    let tag = config.rules[0].filter.as_ref().unwrap().tag.as_ref().unwrap();
    assert_eq!(tag.key, None);
    assert_eq!(tag.value, None);
    assert_eq!(config.encode_config_xml().unwrap(), KEYLESS_XML);
}

#[test]
fn empty_tag_key_and_value_survive() {
    let config = read_xml(EMPTY_KEY_XML);
    let tag = config.rules[0].filter.as_ref().unwrap().tag.as_ref().unwrap();
    assert_eq!(tag.key.as_deref(), Some(""));
    assert_eq!(tag.value.as_deref(), Some(""));
    assert_eq!(config.encode_config_xml().unwrap(), EMPTY_KEY_XML);
}

#[test]
fn the_scanner_cache_distinguishes_empty_and_absent_tag_lists() {
    for (bytes, expected) in [(EMPTY_TAGS_CACHE, Some(Vec::new())), (ABSENT_TAGS_CACHE, None)] {
        let config = read_cache(bytes);
        let and = config.rules[0].filter.as_ref().unwrap().and.as_ref().unwrap();
        assert_eq!(and.tags, expected);
    }
}

#[test]
fn unknown_rule_status_keeps_its_spelling() {
    let config = read_xml(LOWERCASE_XML);
    assert_eq!(config.rules[0].status, "enabled");
    assert_eq!(config.encode_config_xml().unwrap(), LOWERCASE_XML);
}

#[test]
fn a_delete_marker_wrapper_can_lack_status() {
    let config = read_xml(MARKER_XML);
    assert_eq!(
        config.rules[0].delete_marker_replication,
        Some(PersistedOptionalReplicationStatus { status: None })
    );
    assert_eq!(config.encode_config_xml().unwrap(), MARKER_XML);
}

#[test]
fn absent_storage_class_survives() {
    let config = read_xml(MINIMAL_XML);
    assert_eq!(config.rules[0].destination.storage_class, None);
    assert_eq!(config.encode_config_xml().unwrap(), MINIMAL_XML);
}

#[test]
fn bucket_metadata_refuses_historically_invalid_documents() {
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
        assert!(
            PersistedReplicationConfiguration::decode_config_xml(input).is_err(),
            "{}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn an_unknown_root_child_is_skipped() {
    let input = b"<ReplicationConfiguration><Role></Role><FutureTopLevel>x</FutureTopLevel><Rule><Status>Enabled</Status><Destination><Bucket>b</Bucket></Destination></Rule></ReplicationConfiguration>";
    let config = read_xml(input);
    assert_eq!(config.rules.len(), 1);
    assert_eq!(config.rules[0].destination.bucket, "b");
    assert_eq!(config.rules[0].status, "Enabled");
}
