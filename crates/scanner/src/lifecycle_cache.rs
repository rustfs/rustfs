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

//! Lifecycle's legacy serde layout inside the scanner usage cache.
//! This adapter keeps the old field order and millisecond date writer separate
//! from the raw bucket-metadata XML codec and from domain evaluation.

use rustfs_gateway_types::persistence::{
    PersistedAbortIncompleteMultipartUpload, PersistedDelMarkerExpiration, PersistedLifecycleAnd,
    PersistedLifecycleConfiguration, PersistedLifecycleExpiration, PersistedLifecycleFilter, PersistedLifecycleRule,
    PersistedLifecycleTag, PersistedNoncurrentVersionExpiration, PersistedNoncurrentVersionTransition, PersistedTransition,
};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

pub(crate) struct LifecycleCache<'a>(pub(crate) &'a Option<Arc<PersistedLifecycleConfiguration>>);

impl Serialize for LifecycleCache<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_deref().map(CacheConfiguration::from).serialize(serializer)
    }
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Arc<PersistedLifecycleConfiguration>>, D::Error> {
    Ok(Option::<CacheConfiguration>::deserialize(deserializer)?.map(|value| Arc::new(value.into())))
}

#[derive(Serialize, Deserialize)]
struct CacheConfiguration {
    #[serde(default, with = "cache_date")]
    expiry_updated_at: Option<String>,
    rules: Vec<CacheRule>,
}

#[derive(Serialize, Deserialize)]
struct CacheRule {
    abort_incomplete_multipart_upload: Option<CacheAbort>,
    del_marker_expiration: Option<CacheDelMarker>,
    expiration: Option<CacheExpiration>,
    filter: Option<CacheFilter>,
    id: Option<String>,
    noncurrent_version_expiration: Option<CacheNoncurrentExpiration>,
    noncurrent_version_transitions: Option<Vec<CacheNoncurrentTransition>>,
    prefix: Option<String>,
    status: String,
    transitions: Option<Vec<CacheTransition>>,
}

#[derive(Serialize, Deserialize)]
struct CacheAbort {
    days_after_initiation: Option<i32>,
}

#[derive(Serialize, Deserialize)]
struct CacheDelMarker {
    days: Option<i32>,
}

#[derive(Serialize, Deserialize)]
struct CacheExpiration {
    #[serde(default, with = "cache_date")]
    date: Option<String>,
    days: Option<i32>,
    expired_object_all_versions: Option<bool>,
    expired_object_delete_marker: Option<bool>,
}

#[derive(Serialize, Deserialize)]
struct CacheFilter {
    and: Option<CacheAnd>,
    cached_tags: EmptyTagCache,
    object_size_greater_than: Option<i64>,
    object_size_less_than: Option<i64>,
    prefix: Option<String>,
    tag: Option<CacheTag>,
}

#[derive(Serialize, Deserialize)]
struct CacheAnd {
    object_size_greater_than: Option<i64>,
    object_size_less_than: Option<i64>,
    prefix: Option<String>,
    tags: Option<Vec<CacheTag>>,
}

#[derive(Serialize, Deserialize)]
struct CacheTag {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheNoncurrentExpiration {
    newer_noncurrent_versions: Option<i32>,
    noncurrent_days: Option<i32>,
}

#[derive(Serialize, Deserialize)]
struct CacheNoncurrentTransition {
    newer_noncurrent_versions: Option<i32>,
    noncurrent_days: Option<i32>,
    storage_class: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheTransition {
    #[serde(default, with = "cache_date")]
    date: Option<String>,
    days: Option<i32>,
    storage_class: Option<String>,
}

// The pinned s3s handwritten CachedTags implementation writes an empty map,
// accepts only String/String maps on read, and discards their entries.
struct EmptyTagCache;

impl Serialize for EmptyTagCache {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_map(Some(0))?.end()
    }
}

impl<'de> Deserialize<'de> for EmptyTagCache {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TagCacheVisitor;
        impl<'de> Visitor<'de> for TagCacheVisitor {
            type Value = EmptyTagCache;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a map")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
                while access.next_entry::<String, String>()?.is_some() {}
                Ok(EmptyTagCache)
            }
        }
        deserializer.deserialize_map(TagCacheVisitor)
    }
}

mod cache_date {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    pub(super) fn serialize<S: Serializer>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error> {
        let formatted = value
            .as_deref()
            .map(|value| -> Result<String, S::Error> {
                let date = OffsetDateTime::parse(value, &Rfc3339).map_err(serde::ser::Error::custom)?;
                if !(0..=9999).contains(&date.year()) {
                    return Err(serde::ser::Error::custom("lifecycle date year is outside 0..=9999"));
                }
                // The old Timestamp writer formats the stored clock fields with
                // three fraction digits and a literal Z; it does not change offset.
                date.format(time::macros::format_description!(
                    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
                ))
                .map_err(serde::ser::Error::custom)
            })
            .transpose()?;
        formatted.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .as_deref()
            .map(|value| OffsetDateTime::parse(value, &Rfc3339))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

impl From<&PersistedLifecycleConfiguration> for CacheConfiguration {
    fn from(v: &PersistedLifecycleConfiguration) -> Self {
        Self {
            expiry_updated_at: v.expiry_updated_at.clone(),
            rules: v.rules.iter().map(CacheRule::from).collect(),
        }
    }
}

impl From<CacheConfiguration> for PersistedLifecycleConfiguration {
    fn from(v: CacheConfiguration) -> Self {
        Self {
            expiry_updated_at: v.expiry_updated_at,
            rules: v.rules.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<&PersistedLifecycleRule> for CacheRule {
    fn from(v: &PersistedLifecycleRule) -> Self {
        Self {
            abort_incomplete_multipart_upload: v.abort_incomplete_multipart_upload.as_ref().map(|v| CacheAbort {
                days_after_initiation: v.days_after_initiation,
            }),
            del_marker_expiration: v.del_marker_expiration.as_ref().map(|v| CacheDelMarker { days: v.days }),
            expiration: v.expiration.as_ref().map(|v| CacheExpiration {
                date: v.date.clone(),
                days: v.days,
                expired_object_all_versions: v.expired_object_all_versions,
                expired_object_delete_marker: v.expired_object_delete_marker,
            }),
            filter: v.filter.as_ref().map(CacheFilter::from),
            id: v.id.clone(),
            noncurrent_version_expiration: v.noncurrent_version_expiration.as_ref().map(|v| CacheNoncurrentExpiration {
                newer_noncurrent_versions: v.newer_noncurrent_versions,
                noncurrent_days: v.noncurrent_days,
            }),
            noncurrent_version_transitions: v.noncurrent_version_transitions.as_ref().map(|values| {
                values
                    .iter()
                    .map(|v| CacheNoncurrentTransition {
                        newer_noncurrent_versions: v.newer_noncurrent_versions,
                        noncurrent_days: v.noncurrent_days,
                        storage_class: v.storage_class.clone(),
                    })
                    .collect()
            }),
            prefix: v.prefix.clone(),
            status: v.status.clone(),
            transitions: v.transitions.as_ref().map(|values| {
                values
                    .iter()
                    .map(|v| CacheTransition {
                        date: v.date.clone(),
                        days: v.days,
                        storage_class: v.storage_class.clone(),
                    })
                    .collect()
            }),
        }
    }
}

impl From<CacheRule> for PersistedLifecycleRule {
    fn from(v: CacheRule) -> Self {
        Self {
            abort_incomplete_multipart_upload: v.abort_incomplete_multipart_upload.map(|v| {
                PersistedAbortIncompleteMultipartUpload {
                    days_after_initiation: v.days_after_initiation,
                }
            }),
            del_marker_expiration: v.del_marker_expiration.map(|v| PersistedDelMarkerExpiration { days: v.days }),
            expiration: v.expiration.map(|v| PersistedLifecycleExpiration {
                date: v.date,
                days: v.days,
                expired_object_all_versions: v.expired_object_all_versions,
                expired_object_delete_marker: v.expired_object_delete_marker,
            }),
            filter: v.filter.map(Into::into),
            id: v.id,
            noncurrent_version_expiration: v.noncurrent_version_expiration.map(|v| PersistedNoncurrentVersionExpiration {
                newer_noncurrent_versions: v.newer_noncurrent_versions,
                noncurrent_days: v.noncurrent_days,
            }),
            noncurrent_version_transitions: v.noncurrent_version_transitions.map(|values| {
                values
                    .into_iter()
                    .map(|v| PersistedNoncurrentVersionTransition {
                        newer_noncurrent_versions: v.newer_noncurrent_versions,
                        noncurrent_days: v.noncurrent_days,
                        storage_class: v.storage_class,
                    })
                    .collect()
            }),
            prefix: v.prefix,
            status: v.status,
            transitions: v.transitions.map(|values| {
                values
                    .into_iter()
                    .map(|v| PersistedTransition {
                        date: v.date,
                        days: v.days,
                        storage_class: v.storage_class,
                    })
                    .collect()
            }),
        }
    }
}

impl From<&PersistedLifecycleFilter> for CacheFilter {
    fn from(v: &PersistedLifecycleFilter) -> Self {
        Self {
            and: v.and.as_ref().map(|v| CacheAnd {
                object_size_greater_than: v.object_size_greater_than,
                object_size_less_than: v.object_size_less_than,
                prefix: v.prefix.clone(),
                tags: v.tags.as_ref().map(|values| {
                    values
                        .iter()
                        .map(|v| CacheTag {
                            key: v.key.clone(),
                            value: v.value.clone(),
                        })
                        .collect()
                }),
            }),
            cached_tags: EmptyTagCache,
            object_size_greater_than: v.object_size_greater_than,
            object_size_less_than: v.object_size_less_than,
            prefix: v.prefix.clone(),
            tag: v.tag.as_ref().map(|v| CacheTag {
                key: v.key.clone(),
                value: v.value.clone(),
            }),
        }
    }
}

impl From<CacheFilter> for PersistedLifecycleFilter {
    fn from(v: CacheFilter) -> Self {
        Self {
            and: v.and.map(|v| PersistedLifecycleAnd {
                object_size_greater_than: v.object_size_greater_than,
                object_size_less_than: v.object_size_less_than,
                prefix: v.prefix,
                tags: v.tags.map(|values| {
                    values
                        .into_iter()
                        .map(|v| PersistedLifecycleTag {
                            key: v.key,
                            value: v.value,
                        })
                        .collect()
                }),
            }),
            object_size_greater_than: v.object_size_greater_than,
            object_size_less_than: v.object_size_less_than,
            prefix: v.prefix,
            tag: v.tag.map(|v| PersistedLifecycleTag {
                key: v.key,
                value: v.value,
            }),
        }
    }
}
