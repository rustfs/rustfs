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

//! Converts only the lifecycle family between the legacy S3 edge and its persistence shape.
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
fn tag_to_s3s(value: PersistedLifecycleTag) -> dto::Tag {
    dto::Tag {
        key: value.key,
        value: value.value,
    }
}
