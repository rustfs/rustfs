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

//! Converts only the lifecycle family between the legacy S3 edge and its persistence shape. Answers
//! cross the gateway seam instead (`app::bucket_usecase::lifecycle`); the reverse direction is
//! compiled only as the oracle of the legacy answer-parity test and goes with the forward one.
//! Dates keep their original precision and offset until an XML or cache encoder runs.

use rustfs_gateway_types::persistence::{
    PersistedAbortIncompleteMultipartUpload, PersistedDelMarkerExpiration, PersistedLifecycleAnd,
    PersistedLifecycleConfiguration, PersistedLifecycleExpiration, PersistedLifecycleFilter, PersistedLifecycleRule,
    PersistedLifecycleTag, PersistedNoncurrentVersionExpiration, PersistedNoncurrentVersionTransition, PersistedTransition,
};
use s3s::dto;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn date_from_s3s(value: Option<dto::Timestamp>) -> std::io::Result<Option<String>> {
    value
        .map(|value| {
            OffsetDateTime::from(value)
                .format(&Rfc3339)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        })
        .transpose()
}

#[cfg(test)]
fn date_to_s3s(value: Option<String>) -> std::io::Result<Option<dto::Timestamp>> {
    value
        .map(|value| {
            OffsetDateTime::parse(&value, &Rfc3339)
                .map(dto::Timestamp::from)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        })
        .transpose()
}

pub(crate) fn lifecycle_configuration_from_s3s(
    value: dto::BucketLifecycleConfiguration,
) -> std::io::Result<PersistedLifecycleConfiguration> {
    Ok(PersistedLifecycleConfiguration {
        expiry_updated_at: date_from_s3s(value.expiry_updated_at)?,
        rules: value.rules.into_iter().map(rule_from_s3s).collect::<std::io::Result<_>>()?,
    })
}

#[cfg(test)]
pub(crate) fn lifecycle_configuration_to_s3s(
    value: PersistedLifecycleConfiguration,
) -> std::io::Result<dto::BucketLifecycleConfiguration> {
    Ok(dto::BucketLifecycleConfiguration {
        expiry_updated_at: date_to_s3s(value.expiry_updated_at)?,
        rules: value.rules.into_iter().map(rule_to_s3s).collect::<std::io::Result<_>>()?,
    })
}

fn rule_from_s3s(value: dto::LifecycleRule) -> std::io::Result<PersistedLifecycleRule> {
    Ok(PersistedLifecycleRule {
        abort_incomplete_multipart_upload: value.abort_incomplete_multipart_upload.map(|v| {
            PersistedAbortIncompleteMultipartUpload {
                days_after_initiation: v.days_after_initiation,
            }
        }),
        del_marker_expiration: value
            .del_marker_expiration
            .map(|v| PersistedDelMarkerExpiration { days: v.days }),
        expiration: value
            .expiration
            .map(|v| -> std::io::Result<PersistedLifecycleExpiration> {
                Ok(PersistedLifecycleExpiration {
                    date: date_from_s3s(v.date)?,
                    days: v.days,
                    expired_object_all_versions: v.expired_object_all_versions,
                    expired_object_delete_marker: v.expired_object_delete_marker,
                })
            })
            .transpose()?,
        filter: value.filter.map(|v| PersistedLifecycleFilter {
            and: v.and.map(|v| PersistedLifecycleAnd {
                object_size_greater_than: v.object_size_greater_than,
                object_size_less_than: v.object_size_less_than,
                prefix: v.prefix,
                tags: v.tags.map(|tags| tags.into_iter().map(tag_from_s3s).collect()),
            }),
            object_size_greater_than: v.object_size_greater_than,
            object_size_less_than: v.object_size_less_than,
            prefix: v.prefix,
            tag: v.tag.map(tag_from_s3s),
        }),
        id: value.id,
        noncurrent_version_expiration: value
            .noncurrent_version_expiration
            .map(|v| PersistedNoncurrentVersionExpiration {
                newer_noncurrent_versions: v.newer_noncurrent_versions,
                noncurrent_days: v.noncurrent_days,
            }),
        noncurrent_version_transitions: value.noncurrent_version_transitions.map(|values| {
            values
                .into_iter()
                .map(|v| PersistedNoncurrentVersionTransition {
                    newer_noncurrent_versions: v.newer_noncurrent_versions,
                    noncurrent_days: v.noncurrent_days,
                    storage_class: v.storage_class.map(|v| v.as_str().to_owned()),
                })
                .collect()
        }),
        prefix: value.prefix,
        status: value.status.as_str().to_owned(),
        transitions: value
            .transitions
            .map(|values| {
                values
                    .into_iter()
                    .map(|v| -> std::io::Result<PersistedTransition> {
                        Ok(PersistedTransition {
                            date: date_from_s3s(v.date)?,
                            days: v.days,
                            storage_class: v.storage_class.map(|v| v.as_str().to_owned()),
                        })
                    })
                    .collect::<std::io::Result<_>>()
            })
            .transpose()?,
    })
}

#[cfg(test)]
fn rule_to_s3s(value: PersistedLifecycleRule) -> std::io::Result<dto::LifecycleRule> {
    Ok(dto::LifecycleRule {
        abort_incomplete_multipart_upload: value
            .abort_incomplete_multipart_upload
            .map(|v| dto::AbortIncompleteMultipartUpload {
                days_after_initiation: v.days_after_initiation,
            }),
        del_marker_expiration: value.del_marker_expiration.map(|v| dto::DelMarkerExpiration { days: v.days }),
        expiration: value
            .expiration
            .map(|v| -> std::io::Result<dto::LifecycleExpiration> {
                Ok(dto::LifecycleExpiration {
                    date: date_to_s3s(v.date)?,
                    days: v.days,
                    expired_object_all_versions: v.expired_object_all_versions,
                    expired_object_delete_marker: v.expired_object_delete_marker,
                })
            })
            .transpose()?,
        filter: value.filter.map(|v| dto::LifecycleRuleFilter {
            and: v.and.map(|v| dto::LifecycleRuleAndOperator {
                object_size_greater_than: v.object_size_greater_than,
                object_size_less_than: v.object_size_less_than,
                prefix: v.prefix,
                tags: v.tags.map(|tags| tags.into_iter().map(tag_to_s3s).collect()),
            }),
            object_size_greater_than: v.object_size_greater_than,
            object_size_less_than: v.object_size_less_than,
            prefix: v.prefix,
            tag: v.tag.map(tag_to_s3s),
            cached_tags: Default::default(),
        }),
        id: value.id,
        noncurrent_version_expiration: value.noncurrent_version_expiration.map(|v| dto::NoncurrentVersionExpiration {
            newer_noncurrent_versions: v.newer_noncurrent_versions,
            noncurrent_days: v.noncurrent_days,
        }),
        noncurrent_version_transitions: value.noncurrent_version_transitions.map(|values| {
            values
                .into_iter()
                .map(|v| dto::NoncurrentVersionTransition {
                    newer_noncurrent_versions: v.newer_noncurrent_versions,
                    noncurrent_days: v.noncurrent_days,
                    storage_class: v.storage_class.map(Into::into),
                })
                .collect()
        }),
        prefix: value.prefix,
        status: value.status.into(),
        transitions: value
            .transitions
            .map(|values| {
                values
                    .into_iter()
                    .map(|v| -> std::io::Result<dto::Transition> {
                        Ok(dto::Transition {
                            date: date_to_s3s(v.date)?,
                            days: v.days,
                            storage_class: v.storage_class.map(Into::into),
                        })
                    })
                    .collect::<std::io::Result<_>>()
            })
            .transpose()?,
    })
}

fn tag_from_s3s(value: dto::Tag) -> PersistedLifecycleTag {
    PersistedLifecycleTag {
        key: value.key,
        value: value.value,
    }
}
#[cfg(test)]
fn tag_to_s3s(value: PersistedLifecycleTag) -> dto::Tag {
    dto::Tag {
        key: value.key,
        value: value.value,
    }
}

#[cfg(test)]
mod tests {
    use super::lifecycle_configuration_to_s3s;
    use crate::app::dto_values::stored_config;
    use crate::storage::storage_api::ecstore_bucket::utils::BucketConfigXml;
    use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::get_bucket_lifecycle_configuration::output_to_s3s;
    use rustfs_gateway_types::dto as gateway_dto;
    use rustfs_gateway_types::persistence::{PersistedLifecycleConfiguration, parse_lifecycle_dto};
    use s3s::{dto, xml};

    /// Every member the legacy answer carries, the MinIO ones and dates with an offset included.
    const STORED: &[u8] = b"<LifecycleConfiguration><Rule><DelMarkerExpiration><Days>7</Days></DelMarkerExpiration>\
<Expiration><Days>30</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration><Filter><And><Prefix>logs/\
</Prefix><Tag><Key>tier</Key><Value>cold</Value></Tag><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan></And></Filter>\
<ID>members</ID><NoncurrentVersionExpiration><NoncurrentDays>7</NoncurrentDays><NewerNoncurrentVersions>2\
</NewerNoncurrentVersions></NoncurrentVersionExpiration><NoncurrentVersionTransition><NoncurrentDays>3</NoncurrentDays>\
<StorageClass>WARM</StorageClass></NoncurrentVersionTransition><Status>Enabled</Status><Transition><Days>10</Days>\
<StorageClass>WARM</StorageClass></Transition></Rule><Rule><AbortIncompleteMultipartUpload><DaysAfterInitiation>3\
</DaysAfterInitiation></AbortIncompleteMultipartUpload><Expiration><Date>2099-01-01T00:00:00Z</Date></Expiration><ID>dated\
</ID><Prefix>archive/</Prefix><Status>Disabled</Status><Transition><Date>2098-06-01T08:00:00+08:00</Date><StorageClass>WARM\
</StorageClass></Transition></Rule></LifecycleConfiguration>";

    fn s3s_bytes(value: &dto::GetBucketLifecycleConfigurationOutput) -> String {
        let mut buf = Vec::new();
        xml::Serialize::serialize(value, &mut xml::Serializer::new(&mut buf)).expect("the s3s encoder writes the answer");
        String::from_utf8(buf).expect("the s3s encoder writes UTF-8")
    }

    /// The legacy stack answers a stored configuration through the gateway seam with the bytes it
    /// answered through this module's reverse direction.
    #[test]
    fn the_legacy_answer_through_the_seam_is_the_answer_of_the_reverse_direction() {
        let stored = PersistedLifecycleConfiguration::decode_config_xml(STORED).expect("the fixture is a stored document");
        let before = dto::GetBucketLifecycleConfigurationOutput {
            rules: Some(
                lifecycle_configuration_to_s3s(stored.clone())
                    .expect("reverse direction")
                    .rules,
            ),
            ..Default::default()
        };
        let answer = gateway_dto::GetBucketLifecycleConfigurationOutput {
            rules: stored_config(&stored, parse_lifecycle_dto).expect("gateway answer").rules,
            ..Default::default()
        };

        let after = s3s_bytes(&output_to_s3s(answer).expect("seam"));

        assert_eq!(after, s3s_bytes(&before));
        for member in [
            "<DelMarkerExpiration>",
            "<ExpiredObjectAllVersions>",
            "<Transition>",
            "<Date>2098-06-01T",
        ] {
            assert!(after.contains(member), "{member} is missing from {after}");
        }
    }
}
