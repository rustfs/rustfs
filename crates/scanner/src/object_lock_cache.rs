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

//! Object Lock's legacy serde layout inside the scanner usage cache.
//!
//! Responsible for: writing and reading the bucket Object Lock configuration
//! in `DataUsageCacheInfo` with the exact MessagePack layout the s3s DTO's
//! derived serde produced (positional fields, the enabled flag and the
//! retention mode as their wire text), so a cache written before
//! rustfs/backlog#2745 reads back and a cache written now reads on the old
//! binary. Not responsible for: the configuration's XML persistence, which
//! `rustfs_gateway_types::persistence` owns.

use rustfs_gateway_types::persistence::{PersistedDefaultRetention, PersistedObjectLockConfiguration, PersistedObjectLockRule};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

pub(crate) struct ObjectLockCache<'a>(pub(crate) &'a Option<Arc<PersistedObjectLockConfiguration>>);

impl Serialize for ObjectLockCache<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_deref().map(CacheConfiguration::from).serialize(serializer)
    }
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Arc<PersistedObjectLockConfiguration>>, D::Error> {
    Ok(Option::<CacheConfiguration>::deserialize(deserializer)?.map(|value| Arc::new(value.into())))
}

// Field order is the s3s DTO's declaration order: the cache is positional.
#[derive(Serialize, Deserialize)]
struct CacheConfiguration {
    object_lock_enabled: Option<String>,
    rule: Option<CacheRule>,
}

#[derive(Serialize, Deserialize)]
struct CacheRule {
    default_retention: Option<CacheDefaultRetention>,
}

#[derive(Serialize, Deserialize)]
struct CacheDefaultRetention {
    days: Option<i32>,
    mode: Option<String>,
    years: Option<i32>,
}

impl From<&PersistedObjectLockConfiguration> for CacheConfiguration {
    fn from(value: &PersistedObjectLockConfiguration) -> Self {
        Self {
            object_lock_enabled: value.object_lock_enabled.clone(),
            rule: value.rule.as_ref().map(|rule| CacheRule {
                default_retention: rule.default_retention.as_ref().map(|retention| CacheDefaultRetention {
                    days: retention.days,
                    mode: retention.mode.clone(),
                    years: retention.years,
                }),
            }),
        }
    }
}

impl From<CacheConfiguration> for PersistedObjectLockConfiguration {
    fn from(value: CacheConfiguration) -> Self {
        Self {
            object_lock_enabled: value.object_lock_enabled,
            rule: value.rule.map(|rule| PersistedObjectLockRule {
                default_retention: rule.default_retention.map(|retention| PersistedDefaultRetention {
                    mode: retention.mode,
                    days: retention.days,
                    years: retention.years,
                }),
            }),
        }
    }
}
