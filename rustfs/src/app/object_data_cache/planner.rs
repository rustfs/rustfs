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
use crate::storage::storage_api::ecstore_bucket::lifecycle::bucket_lifecycle_ops::LifecycleOps as _;
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
        // Remote (transitioned) objects are served from the warm tier; the
        // ecstore hook already refuses them (hook.rs is_remote()), so the
        // usecase-layer planner must exclude them too for a uniform contract.
        || request.info.is_remote()
        // Zero-length bodies save no I/O — ecstore returns an empty body before
        // the hook probe — so admitting them only creates useless entries and
        // inflates hit metrics. Mirrors should_buffer_get_object_in_memory_with_threshold.
        || request.response_content_length <= 0
    {
        return GetObjectBodyCachePlan::Skip;
    }

    let Some(etag) = request.info.etag.as_deref() else {
        return GetObjectBodyCachePlan::Skip;
    };

    let Ok(size) = u64::try_from(request.response_content_length) else {
        return GetObjectBodyCachePlan::Skip;
    };

    // Nil version ids mean "no value" (see CLAUDE.md); map them to None so the
    // engine canonicalizes to the same "null" key as unversioned reads.
    let version_id = request
        .info
        .version_id
        .filter(|version_id| !version_id.is_nil())
        .map(|version_id| version_id.to_string());
    // ODC-06 (backlog#1111): carry the resolved version's modification time into
    // the key so an unversioned overwrite (which advances mod_time) cannot be
    // served the stale body under an MD5 collision. Absent mod_time maps to 0.
    let mod_time_unix_nanos = request
        .info
        .mod_time
        .map(|mod_time| mod_time.unix_timestamp_nanos())
        .unwrap_or(0);
    let engine_request = ObjectDataCacheGetRequest {
        bucket: request.bucket,
        object: request.key,
        version_id,
        etag,
        size,
        mod_time_unix_nanos,
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
    fn plan_skips_remote_transitioned_objects() {
        // backlog#1138: transitioned (remote-tier) objects are served from the
        // warm backend; the ecstore hook already refuses them, so the
        // usecase-layer planner must exclude them too for a uniform contract.
        let adapter = enabled_adapter();
        let mut info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            ..Default::default()
        };
        info.transitioned_object.status = "complete".to_string();

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
    fn plan_skips_zero_length_objects() {
        // backlog#1142: ecstore returns an empty body before the hook probe, so
        // a zero-length GET saves no I/O; admitting it only inflates hit metrics.
        let adapter = enabled_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 0,
            ..Default::default()
        };

        let plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &info,
                response_content_length: 0,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );

        assert!(matches!(plan, GetObjectBodyCachePlan::Skip));
    }

    fn cacheable_key(plan: &GetObjectBodyCachePlan) -> rustfs_object_data_cache::ObjectDataCacheKey {
        match plan {
            GetObjectBodyCachePlan::Cacheable(rustfs_object_data_cache::ObjectDataCacheGetPlan::Cacheable { key }) => key.clone(),
            other => panic!("expected a cacheable plan, got {other:?}"),
        }
    }

    #[test]
    fn hook_and_planner_derive_identical_keys() {
        // ODC-16 correctness guard: the ecstore hook and the usecase planner both
        // route through build_get_object_body_cache_plan, so for the same object
        // they must derive byte-identical keys — otherwise every GET misses. The
        // hook builds its request with response_content_length = get_actual_size;
        // the usecase builds the same for a plain, non-range GET.
        let adapter = enabled_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            actual_size: 4,
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp_nanos(1_234_567_890).unwrap()),
            ..Default::default()
        };

        let hook_request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: info.get_actual_size().expect("actual size"),
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let usecase_request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: 4,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };

        let hook_key = cacheable_key(&build_get_object_body_cache_plan(&adapter, hook_request));
        let usecase_key = cacheable_key(&build_get_object_body_cache_plan(&adapter, usecase_request));

        assert_eq!(hook_key, usecase_key, "hook and planner must derive the same key");
        // ODC-06: the modification time flows into the key.
        assert_eq!(hook_key.mod_time_unix_nanos, 1_234_567_890);
    }

    #[test]
    fn planner_key_changes_with_mod_time() {
        // ODC-06 (backlog#1111): an unversioned overwrite advances mod_time, so
        // the same etag + size must derive a different key.
        let adapter = enabled_adapter();
        let mut info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            actual_size: 4,
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp_nanos(1_000).unwrap()),
            ..Default::default()
        };
        let make_request = |info: &crate::storage::storage_api::StorageObjectInfo| {
            build_get_object_body_cache_plan(
                &adapter,
                GetObjectBodyCacheRequest {
                    bucket: "bucket",
                    key: "object",
                    info,
                    response_content_length: 4,
                    has_range: false,
                    part_number: None,
                    encryption_applied: false,
                },
            )
        };
        let old_key = cacheable_key(&make_request(&info));
        info.mod_time = Some(time::OffsetDateTime::from_unix_timestamp_nanos(2_000).unwrap());
        let new_key = cacheable_key(&make_request(&info));

        assert_ne!(old_key, new_key, "keys differing only by mod_time must not collide");
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
