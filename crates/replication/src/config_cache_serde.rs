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

//! The serde form of a replication configuration inside the scanner's data
//! usage cache.
//!
//! Responsible for: encoding `PersistedReplicationConfiguration` with serde in
//! exactly the layout the s3s DTO's derived serde produced, because the
//! ECStore `ReplicationConfig` carrying it is a member of the scanner's
//! `DataUsageCacheInfo`, which RustFS persists with MessagePack: a cache written
//! before the gateway types must still decode, and an older reader must still
//! decode a cache written now.
//!
//! Not responsible for: the persisted bucket-metadata XML (that is the gateway
//! persistence codec) or deciding whether the cache should carry the
//! configuration at all.
//!
//! The layout is the derived one of the s3s `minio` DTOs: every struct is its
//! fields in declaration order (alphabetical), every string enum is its wire
//! text, and the rule filter carries the s3s tag cache as an empty map.
//!
//! Upstream: the serde derive of ECStore's `ReplicationConfig`, through
//! `#[serde(with = ...)]`. Downstream: none.

use rustfs_gateway_types::persistence::{
    PersistedAccessControlTranslation, PersistedEncryptionConfiguration, PersistedOptionalReplicationStatus,
    PersistedReplicationAnd, PersistedReplicationConfiguration, PersistedReplicationDestination, PersistedReplicationFilter,
    PersistedReplicationMetrics, PersistedReplicationRule, PersistedReplicationStatus, PersistedReplicationTag,
    PersistedReplicationTime, PersistedReplicationTimeValue, PersistedSourceSelectionCriteria,
};
use serde::de::IgnoredAny;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S: Serializer>(value: &Option<PersistedReplicationConfiguration>, serializer: S) -> Result<S::Ok, S::Error> {
    value.as_ref().map(CacheConfiguration::from).serialize(serializer)
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<PersistedReplicationConfiguration>, D::Error> {
    Ok(Option::<CacheConfiguration>::deserialize(deserializer)?.map(Into::into))
}

#[derive(Serialize, Deserialize)]
struct CacheConfiguration {
    role: String,
    rules: Vec<CacheRule>,
}

#[derive(Serialize, Deserialize)]
struct CacheRule {
    delete_marker_replication: Option<CacheOptionalStatus>,
    delete_replication: Option<CacheStatus>,
    destination: CacheDestination,
    existing_object_replication: Option<CacheStatus>,
    filter: Option<CacheFilter>,
    id: Option<String>,
    prefix: Option<String>,
    priority: Option<i32>,
    source_selection_criteria: Option<CacheSourceSelection>,
    status: String,
}

#[derive(Serialize, Deserialize)]
struct CacheOptionalStatus {
    status: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheStatus {
    status: String,
}

#[derive(Serialize, Deserialize)]
struct CacheDestination {
    access_control_translation: Option<CacheAccessControlTranslation>,
    account: Option<String>,
    bucket: String,
    encryption_configuration: Option<CacheEncryptionConfiguration>,
    metrics: Option<CacheMetrics>,
    replication_time: Option<CacheReplicationTime>,
    storage_class: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheAccessControlTranslation {
    owner: String,
}

#[derive(Serialize, Deserialize)]
struct CacheEncryptionConfiguration {
    replica_kms_key_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheMetrics {
    event_threshold: Option<CacheTimeValue>,
    status: String,
}

#[derive(Serialize, Deserialize)]
struct CacheReplicationTime {
    status: String,
    time: CacheTimeValue,
}

#[derive(Serialize, Deserialize)]
struct CacheTimeValue {
    minutes: Option<i32>,
}

#[derive(Serialize, Deserialize)]
struct CacheFilter {
    and: Option<CacheAnd>,
    cached_tags: EmptyTagCache,
    prefix: Option<String>,
    tag: Option<CacheTag>,
}

#[derive(Serialize, Deserialize)]
struct CacheAnd {
    prefix: Option<String>,
    tags: Option<Vec<CacheTag>>,
}

#[derive(Serialize, Deserialize)]
struct CacheTag {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheSourceSelection {
    replica_modifications: Option<CacheStatus>,
    sse_kms_encrypted_objects: Option<CacheStatus>,
}

/// The s3s rule filter's parse cache: written as an empty map, and whatever
/// an older writer stored there is read and dropped.
struct EmptyTagCache;

impl Serialize for EmptyTagCache {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_map(Some(0))?.end()
    }
}

impl<'de> Deserialize<'de> for EmptyTagCache {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

impl From<&PersistedReplicationConfiguration> for CacheConfiguration {
    fn from(value: &PersistedReplicationConfiguration) -> Self {
        Self {
            role: value.role.clone(),
            rules: value.rules.iter().map(CacheRule::from).collect(),
        }
    }
}

impl From<CacheConfiguration> for PersistedReplicationConfiguration {
    fn from(value: CacheConfiguration) -> Self {
        Self {
            role: value.role,
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<&PersistedReplicationRule> for CacheRule {
    fn from(value: &PersistedReplicationRule) -> Self {
        Self {
            delete_marker_replication: value.delete_marker_replication.as_ref().map(|wrapper| CacheOptionalStatus {
                status: wrapper.status.clone(),
            }),
            delete_replication: value.delete_replication.as_ref().map(CacheStatus::from),
            destination: CacheDestination::from(&value.destination),
            existing_object_replication: value.existing_object_replication.as_ref().map(CacheStatus::from),
            filter: value.filter.as_ref().map(CacheFilter::from),
            id: value.id.clone(),
            prefix: value.prefix.clone(),
            priority: value.priority,
            source_selection_criteria: value.source_selection_criteria.as_ref().map(|criteria| CacheSourceSelection {
                replica_modifications: criteria.replica_modifications.as_ref().map(CacheStatus::from),
                sse_kms_encrypted_objects: criteria.sse_kms_encrypted_objects.as_ref().map(CacheStatus::from),
            }),
            status: value.status.clone(),
        }
    }
}

impl From<CacheRule> for PersistedReplicationRule {
    fn from(value: CacheRule) -> Self {
        Self {
            delete_marker_replication: value
                .delete_marker_replication
                .map(|wrapper| PersistedOptionalReplicationStatus { status: wrapper.status }),
            delete_replication: value.delete_replication.map(Into::into),
            destination: value.destination.into(),
            existing_object_replication: value.existing_object_replication.map(Into::into),
            filter: value.filter.map(Into::into),
            id: value.id,
            prefix: value.prefix,
            priority: value.priority,
            source_selection_criteria: value
                .source_selection_criteria
                .map(|criteria| PersistedSourceSelectionCriteria {
                    replica_modifications: criteria.replica_modifications.map(Into::into),
                    sse_kms_encrypted_objects: criteria.sse_kms_encrypted_objects.map(Into::into),
                }),
            status: value.status,
        }
    }
}

impl From<&PersistedReplicationStatus> for CacheStatus {
    fn from(value: &PersistedReplicationStatus) -> Self {
        Self {
            status: value.status.clone(),
        }
    }
}

impl From<CacheStatus> for PersistedReplicationStatus {
    fn from(value: CacheStatus) -> Self {
        Self { status: value.status }
    }
}

impl From<&PersistedReplicationDestination> for CacheDestination {
    fn from(value: &PersistedReplicationDestination) -> Self {
        Self {
            access_control_translation: value.access_control_translation.as_ref().map(|translation| {
                CacheAccessControlTranslation {
                    owner: translation.owner.clone(),
                }
            }),
            account: value.account.clone(),
            bucket: value.bucket.clone(),
            encryption_configuration: value
                .encryption_configuration
                .as_ref()
                .map(|configuration| CacheEncryptionConfiguration {
                    replica_kms_key_id: configuration.replica_kms_key_id.clone(),
                }),
            metrics: value.metrics.as_ref().map(|metrics| CacheMetrics {
                event_threshold: metrics.event_threshold.as_ref().map(CacheTimeValue::from),
                status: metrics.status.clone(),
            }),
            replication_time: value.replication_time.as_ref().map(|replication_time| CacheReplicationTime {
                status: replication_time.status.clone(),
                time: CacheTimeValue::from(&replication_time.time),
            }),
            storage_class: value.storage_class.clone(),
        }
    }
}

impl From<CacheDestination> for PersistedReplicationDestination {
    fn from(value: CacheDestination) -> Self {
        Self {
            account: value.account,
            access_control_translation: value
                .access_control_translation
                .map(|translation| PersistedAccessControlTranslation {
                    owner: translation.owner,
                }),
            bucket: value.bucket,
            encryption_configuration: value
                .encryption_configuration
                .map(|configuration| PersistedEncryptionConfiguration {
                    replica_kms_key_id: configuration.replica_kms_key_id,
                }),
            metrics: value.metrics.map(|metrics| PersistedReplicationMetrics {
                event_threshold: metrics.event_threshold.map(Into::into),
                status: metrics.status,
            }),
            replication_time: value.replication_time.map(|replication_time| PersistedReplicationTime {
                status: replication_time.status,
                time: replication_time.time.into(),
            }),
            storage_class: value.storage_class,
        }
    }
}

impl From<&PersistedReplicationTimeValue> for CacheTimeValue {
    fn from(value: &PersistedReplicationTimeValue) -> Self {
        Self { minutes: value.minutes }
    }
}

impl From<CacheTimeValue> for PersistedReplicationTimeValue {
    fn from(value: CacheTimeValue) -> Self {
        Self { minutes: value.minutes }
    }
}

impl From<&PersistedReplicationFilter> for CacheFilter {
    fn from(value: &PersistedReplicationFilter) -> Self {
        Self {
            and: value.and.as_ref().map(|and| CacheAnd {
                prefix: and.prefix.clone(),
                tags: and.tags.as_ref().map(|tags| tags.iter().map(CacheTag::from).collect()),
            }),
            cached_tags: EmptyTagCache,
            prefix: value.prefix.clone(),
            tag: value.tag.as_ref().map(CacheTag::from),
        }
    }
}

impl From<CacheFilter> for PersistedReplicationFilter {
    fn from(value: CacheFilter) -> Self {
        Self {
            and: value.and.map(|and| PersistedReplicationAnd {
                prefix: and.prefix,
                tags: and.tags.map(|tags| tags.into_iter().map(Into::into).collect()),
            }),
            prefix: value.prefix,
            tag: value.tag.map(Into::into),
        }
    }
}

impl From<&PersistedReplicationTag> for CacheTag {
    fn from(value: &PersistedReplicationTag) -> Self {
        Self {
            key: value.key.clone(),
            value: value.value.clone(),
        }
    }
}

impl From<CacheTag> for PersistedReplicationTag {
    fn from(value: CacheTag) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A newtype carrying the field exactly as `ReplicationConfig` does, so
    /// the bytes below are the ones the cache holds for that field.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Field(#[serde(with = "super")] Option<PersistedReplicationConfiguration>);

    /// The derived s3s serde of `Some(every_member())`, as the data usage cache
    /// writes it (`rmp_serde::Serializer::new`), recorded from s3s rev
    /// 5761ddfe with the `minio` feature.
    const S3S_COMPACT: &str = concat!(
        "92d92a61726e3a6177733a69616d3a3a3132333435363738393031323a726f6c652f7265706c69636174696f6e929a91",
        "a7456e61626c656491a7456e61626c65649791ab44657374696e6174696f6eac313233343536373839303132b361726e",
        "3a6177733a73333a3a3a6261636b757091a76b6d732d6b657992910fa7456e61626c656492a7456e61626c6564911ea8",
        "5354414e4441524491a844697361626c65649492a5646f63732f9292a47465616da773746f7261676592c0c080a76669",
        "6c7465722f92a3656e76a470726f64ac65766572792d6d656d626572a76c65676163792f079291a7456e61626c656491",
        "a844697361626c6564a7456e61626c65649ac0c097c0c0b361726e3a6177733a73333a3a3a7365636f6e64c0c0c0c0c0",
        "c0c0c0c0c0a844697361626c6564",
    );

    /// The same value written with field names (`rmp_serde::to_vec_named`).
    const S3S_NAMED: &str = concat!(
        "82a4726f6c65d92a61726e3a6177733a69616d3a3a3132333435363738393031323a726f6c652f7265706c6963617469",
        "6f6ea572756c6573928ab964656c6574655f6d61726b65725f7265706c69636174696f6e81a6737461747573a7456e61",
        "626c6564b264656c6574655f7265706c69636174696f6e81a6737461747573a7456e61626c6564ab64657374696e6174",
        "696f6e87ba6163636573735f636f6e74726f6c5f7472616e736c6174696f6e81a56f776e6572ab44657374696e617469",
        "6f6ea76163636f756e74ac313233343536373839303132a66275636b6574b361726e3a6177733a73333a3a3a6261636b",
        "7570b8656e6372797074696f6e5f636f6e66696775726174696f6e81b27265706c6963615f6b6d735f6b65795f6964a7",
        "6b6d732d6b6579a76d65747269637382af6576656e745f7468726573686f6c6481a76d696e757465730fa67374617475",
        "73a7456e61626c6564b07265706c69636174696f6e5f74696d6582a6737461747573a7456e61626c6564a474696d6581",
        "a76d696e757465731ead73746f726167655f636c617373a85354414e44415244bb6578697374696e675f6f626a656374",
        "5f7265706c69636174696f6e81a6737461747573a844697361626c6564a666696c74657284a3616e6482a67072656669",
        "78a5646f63732fa4746167739282a36b6579a47465616da576616c7565a773746f7261676582a36b6579c0a576616c75",
        "65c0ab6361636865645f7461677380a6707265666978a766696c7465722fa374616782a36b6579a3656e76a576616c75",
        "65a470726f64a26964ac65766572792d6d656d626572a6707265666978a76c65676163792fa87072696f7269747907b9",
        "736f757263655f73656c656374696f6e5f637269746572696182b57265706c6963615f6d6f64696669636174696f6e73",
        "81a6737461747573a7456e61626c6564b97373655f6b6d735f656e637279707465645f6f626a6563747381a673746174",
        "7573a844697361626c6564a6737461747573a7456e61626c65648ab964656c6574655f6d61726b65725f7265706c6963",
        "6174696f6ec0b264656c6574655f7265706c69636174696f6ec0ab64657374696e6174696f6e87ba6163636573735f63",
        "6f6e74726f6c5f7472616e736c6174696f6ec0a76163636f756e74c0a66275636b6574b361726e3a6177733a73333a3a",
        "3a7365636f6e64b8656e6372797074696f6e5f636f6e66696775726174696f6ec0a76d657472696373c0b07265706c69",
        "636174696f6e5f74696d65c0ad73746f726167655f636c617373c0bb6578697374696e675f6f626a6563745f7265706c",
        "69636174696f6ec0a666696c746572c0a26964c0a6707265666978c0a87072696f72697479c0b9736f757263655f7365",
        "6c656374696f6e5f6372697465726961c0a6737461747573a844697361626c6564",
    );

    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("fixture hex is valid"))
            .collect()
    }

    fn encode(value: Option<PersistedReplicationConfiguration>) -> Vec<u8> {
        let mut buf = Vec::new();
        Field(value)
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .expect("the cache encoding never fails");
        buf
    }

    fn decode(input: &[u8]) -> Result<Option<PersistedReplicationConfiguration>, rmp_serde::decode::Error> {
        rmp_serde::from_slice::<Field>(input).map(|field| field.0)
    }

    fn status(value: &str) -> PersistedReplicationStatus {
        PersistedReplicationStatus {
            status: value.to_owned(),
        }
    }

    fn tag(key: Option<&str>, value: Option<&str>) -> PersistedReplicationTag {
        PersistedReplicationTag {
            key: key.map(str::to_owned),
            value: value.map(str::to_owned),
        }
    }

    fn every_member() -> PersistedReplicationConfiguration {
        PersistedReplicationConfiguration {
            role: "arn:aws:iam::123456789012:role/replication".to_owned(),
            rules: vec![
                PersistedReplicationRule {
                    delete_marker_replication: Some(PersistedOptionalReplicationStatus {
                        status: Some("Enabled".to_owned()),
                    }),
                    delete_replication: Some(status("Enabled")),
                    destination: PersistedReplicationDestination {
                        account: Some("123456789012".to_owned()),
                        access_control_translation: Some(PersistedAccessControlTranslation {
                            owner: "Destination".to_owned(),
                        }),
                        bucket: "arn:aws:s3:::backup".to_owned(),
                        encryption_configuration: Some(PersistedEncryptionConfiguration {
                            replica_kms_key_id: Some("kms-key".to_owned()),
                        }),
                        metrics: Some(PersistedReplicationMetrics {
                            event_threshold: Some(PersistedReplicationTimeValue { minutes: Some(15) }),
                            status: "Enabled".to_owned(),
                        }),
                        replication_time: Some(PersistedReplicationTime {
                            status: "Enabled".to_owned(),
                            time: PersistedReplicationTimeValue { minutes: Some(30) },
                        }),
                        storage_class: Some("STANDARD".to_owned()),
                    },
                    existing_object_replication: Some(status("Disabled")),
                    filter: Some(PersistedReplicationFilter {
                        and: Some(PersistedReplicationAnd {
                            prefix: Some("docs/".to_owned()),
                            tags: Some(vec![tag(Some("team"), Some("storage")), tag(None, None)]),
                        }),
                        prefix: Some("filter/".to_owned()),
                        tag: Some(tag(Some("env"), Some("prod"))),
                    }),
                    id: Some("every-member".to_owned()),
                    prefix: Some("legacy/".to_owned()),
                    priority: Some(7),
                    source_selection_criteria: Some(PersistedSourceSelectionCriteria {
                        replica_modifications: Some(status("Enabled")),
                        sse_kms_encrypted_objects: Some(status("Disabled")),
                    }),
                    status: "Enabled".to_owned(),
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

    #[test]
    fn writes_the_bytes_the_old_derive_wrote() {
        assert_eq!(encode(Some(every_member())), bytes(S3S_COMPACT));
    }

    #[test]
    fn reads_a_cache_the_old_derive_wrote() {
        assert_eq!(decode(&bytes(S3S_COMPACT)).expect("the old cache field decodes"), Some(every_member()));
        assert_eq!(decode(&bytes(S3S_NAMED)).expect("the named form decodes"), Some(every_member()));
    }

    #[test]
    fn an_absent_configuration_stays_nil() {
        assert_eq!(encode(None), vec![0xc0]);
        assert_eq!(decode(&[0xc0]).expect("nil decodes"), None);
    }

    #[test]
    fn a_filled_tag_cache_from_an_older_writer_is_dropped_not_refused() {
        let mut stored = every_member();
        stored.rules.truncate(1);
        let mut buf = encode(Some(stored.clone()));
        // The filter is the fifth member of the rule; its second member is the
        // empty tag cache map (0x80). Put one entry in it.
        let position = buf
            .windows(2)
            .position(|pair| pair == [0x80, 0xa7])
            .expect("the empty tag cache is written");
        buf.splice(position..=position, [0x81, 0xa1, b'k', 0xa1, b'v']);
        assert_eq!(decode(&buf).expect("a filled tag cache is ignored"), Some(stored));
    }

    #[test]
    fn a_truncated_rule_is_refused_rather_than_defaulted() {
        let mut stored = every_member();
        stored.rules.truncate(1);
        let mut buf = encode(Some(stored));
        // Drop the rule's last member (its status) and shorten its array header.
        buf.truncate(buf.len() - "Enabled".len() - 1);
        let header = buf
            .iter()
            .position(|byte| *byte == 0x9a)
            .expect("the rule is a ten-member array");
        buf[header] = 0x99;
        assert!(decode(&buf).is_err(), "a rule without its status must not decode into a default status");
    }

    #[test]
    fn a_wrong_shape_is_refused() {
        let buf = rmp_serde::to_vec(&"not a configuration").expect("encode");
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn a_status_outside_the_vocabulary_keeps_its_text() {
        let mut stored = every_member();
        stored.rules[0].status = "enabled".to_owned();
        let crossed = decode(&encode(Some(stored.clone()))).expect("round trip");
        assert_eq!(crossed, Some(stored));
    }
}
