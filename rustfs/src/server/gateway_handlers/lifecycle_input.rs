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

//! Converts gateway lifecycle input to the native persisted shape before validation.
//! It preserves request-local dates and their precision for validation in the use case.

use crate::app::bucket_usecase::{PreparedLifecycleConfiguration, validate_lifecycle_rule_status};
use rustfs_gateway::LifecycleRequestDates;
use rustfs_gateway_types::persistence::{
    PersistedAbortIncompleteMultipartUpload, PersistedDelMarkerExpiration, PersistedLifecycleAnd,
    PersistedLifecycleConfiguration, PersistedLifecycleExpiration, PersistedLifecycleFilter, PersistedLifecycleRule,
    PersistedLifecycleTag, PersistedNoncurrentVersionExpiration, PersistedNoncurrentVersionTransition, PersistedTransition,
};
use rustfs_gateway_types::{Timestamp, dto};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn date_from_gateway(value: Option<Timestamp>, source_date: Option<&str>) -> std::io::Result<Option<String>> {
    value
        .map(|value| {
            if let Some(source_date) = source_date {
                return Ok(source_date.to_owned());
            }
            let nanos = i128::from(value.secs()) * 1_000_000_000 + i128::from(value.subsec_nanos());
            OffsetDateTime::from_unix_timestamp_nanos(nanos)
                .map_err(std::io::Error::other)?
                .format(&Rfc3339)
                .map_err(std::io::Error::other)
        })
        .transpose()
}

pub(super) fn prepare_lifecycle_configuration(
    configuration: Option<dto::BucketLifecycleConfiguration>,
    dates: Option<&LifecycleRequestDates>,
) -> PreparedLifecycleConfiguration {
    let Some(configuration) = configuration else {
        return PreparedLifecycleConfiguration::Missing;
    };
    if validate_lifecycle_rule_status(configuration.rules.iter().map(|rule| rule.status.as_str())).is_err() {
        return PreparedLifecycleConfiguration::InvalidStatus;
    }
    PreparedLifecycleConfiguration::Prepared(lifecycle_configuration_from_gateway(configuration, dates))
}

fn lifecycle_configuration_from_gateway(
    value: dto::BucketLifecycleConfiguration,
    dates: Option<&LifecycleRequestDates>,
) -> std::io::Result<PersistedLifecycleConfiguration> {
    Ok(PersistedLifecycleConfiguration {
        expiry_updated_at: date_from_gateway(value.expiry_updated_at, dates.and_then(LifecycleRequestDates::expiry_updated_at))?,
        rules: value
            .rules
            .into_iter()
            .enumerate()
            .map(|(index, rule)| rule_from_gateway(rule, dates, index))
            .collect::<std::io::Result<_>>()?,
    })
}

fn rule_from_gateway(
    value: dto::LifecycleRule,
    dates: Option<&LifecycleRequestDates>,
    rule_index: usize,
) -> std::io::Result<PersistedLifecycleRule> {
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
                    date: date_from_gateway(v.date, dates.and_then(|dates| dates.expiration(rule_index)))?,
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
                tags: (!v.tags.is_empty()).then(|| v.tags.into_iter().map(tag_from_gateway).collect()),
            }),
            object_size_greater_than: v.object_size_greater_than,
            object_size_less_than: v.object_size_less_than,
            prefix: v.prefix,
            tag: v.tag.map(tag_from_gateway),
        }),
        id: value.id,
        noncurrent_version_expiration: value
            .noncurrent_version_expiration
            .map(|v| PersistedNoncurrentVersionExpiration {
                newer_noncurrent_versions: v.newer_noncurrent_versions,
                noncurrent_days: v.noncurrent_days,
            }),
        noncurrent_version_transitions: (!value.noncurrent_version_transitions.is_empty()).then(|| {
            value
                .noncurrent_version_transitions
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
        transitions: (!value.transitions.is_empty())
            .then(|| {
                value
                    .transitions
                    .into_iter()
                    .enumerate()
                    .map(|(index, v)| -> std::io::Result<PersistedTransition> {
                        Ok(PersistedTransition {
                            date: date_from_gateway(v.date, dates.and_then(|dates| dates.transition(rule_index, index)))?,
                            days: v.days,
                            storage_class: v.storage_class.map(|v| v.as_str().to_owned()),
                        })
                    })
                    .collect::<std::io::Result<_>>()
            })
            .transpose()?,
    })
}

fn tag_from_gateway(value: dto::Tag) -> PersistedLifecycleTag {
    PersistedLifecycleTag {
        key: value.key,
        value: value.value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_lifecycle_configuration_checks_all_statuses_before_converting_dates() {
        let mut configuration = dto::BucketLifecycleConfiguration {
            rules: vec![
                dto::LifecycleRule {
                    expiration: Some(dto::LifecycleExpiration {
                        date: Some(Timestamp::from_secs(i64::MAX)),
                        ..Default::default()
                    }),
                    status: "Enabled".into(),
                    ..Default::default()
                },
                dto::LifecycleRule {
                    status: "enabled".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(lifecycle_configuration_from_gateway(configuration.clone(), None).is_err());
        assert!(matches!(
            prepare_lifecycle_configuration(Some(configuration.clone()), None),
            PreparedLifecycleConfiguration::InvalidStatus
        ));

        configuration.rules[1].status = "Disabled".into();
        assert!(matches!(
            prepare_lifecycle_configuration(Some(configuration), None),
            PreparedLifecycleConfiguration::Prepared(Err(_))
        ));
    }
}
