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

//! GET planning glue for the object data cache adapter.

use crate::app::object_data_cache::ObjectDataCacheAdapter;
use crate::app::storage_api::object_usecase::StorageObjectInfo;
use rustfs_object_data_cache::{ObjectDataCacheBodyVariant, ObjectDataCacheGetPlan, ObjectDataCacheGetRequest};

/// App-layer GET request snapshot used for cache planning.
#[derive(Clone, Copy)]
pub(crate) struct GetObjectBodyCacheRequest<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) info: &'a StorageObjectInfo,
    pub(crate) response_content_length: i64,
    pub(crate) has_range: bool,
    pub(crate) part_number: Option<usize>,
    pub(crate) encryption_applied: bool,
}

/// Planning result for app-layer GET cache lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GetObjectBodyCachePlan {
    Disabled,
    Skip,
    Cacheable(ObjectDataCacheGetPlan),
}

/// Builds a conservative body-cache plan for a GET request.
pub(crate) fn build_get_object_body_cache_plan(
    adapter: &ObjectDataCacheAdapter,
    request: GetObjectBodyCacheRequest<'_>,
) -> GetObjectBodyCachePlan {
    if adapter.is_disabled() {
        return GetObjectBodyCachePlan::Disabled;
    }

    if request.has_range
        || request.part_number.is_some()
        || request.encryption_applied
        || request.info.delete_marker
        || request.info.version_only
        || request.info.metadata_only
        || request.response_content_length < 0
    {
        return GetObjectBodyCachePlan::Skip;
    }

    let Some(etag) = request.info.etag.as_deref() else {
        return GetObjectBodyCachePlan::Skip;
    };

    let Ok(size) = u64::try_from(request.response_content_length) else {
        return GetObjectBodyCachePlan::Skip;
    };

    let version_id = request.info.version_id.map(|version_id| version_id.to_string());
    let engine_request = ObjectDataCacheGetRequest {
        bucket: request.bucket,
        object: request.key,
        version_id,
        etag,
        size,
        body_variant: ObjectDataCacheBodyVariant::FullObjectPlainV1,
    };

    match adapter.plan_get(engine_request) {
        ObjectDataCacheGetPlan::Disabled | ObjectDataCacheGetPlan::SkipTooLarge => GetObjectBodyCachePlan::Skip,
        plan @ ObjectDataCacheGetPlan::Cacheable { .. } => GetObjectBodyCachePlan::Cacheable(plan),
    }
}

#[cfg(test)]
mod tests {
    use super::{GetObjectBodyCachePlan, GetObjectBodyCacheRequest, build_get_object_body_cache_plan};
    use crate::app::object_data_cache::ObjectDataCacheAdapter;
    use rustfs_object_data_cache::{ObjectDataCacheConfig, ObjectDataCacheMode};

    fn enabled_adapter() -> ObjectDataCacheAdapter {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 8_388_608,
            ..ObjectDataCacheConfig::default()
        };
        ObjectDataCacheAdapter::new(config).expect("hit-only config should build adapter")
    }

    #[test]
    fn plan_is_disabled_when_adapter_is_disabled() {
        let adapter = ObjectDataCacheAdapter::disabled();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            ..Default::default()
        };

        let plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &info,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );

        assert!(matches!(plan, GetObjectBodyCachePlan::Disabled));
    }

    #[test]
    fn plan_skips_range_requests() {
        let adapter = enabled_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            ..Default::default()
        };

        let plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &info,
                response_content_length: 4,
                has_range: true,
                part_number: None,
                encryption_applied: false,
            },
        );

        assert!(matches!(plan, GetObjectBodyCachePlan::Skip));
    }

    #[test]
    fn plan_skips_when_etag_is_missing() {
        let adapter = enabled_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: None,
            size: 4,
            ..Default::default()
        };

        let plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &info,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );

        assert!(matches!(plan, GetObjectBodyCachePlan::Skip));
    }

    #[test]
    fn plan_is_cacheable_for_plain_full_object() {
        let adapter = enabled_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            ..Default::default()
        };

        let plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &info,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );

        assert!(matches!(plan, GetObjectBodyCachePlan::Cacheable(_)));
    }
}
