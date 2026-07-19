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

impl GetObjectBodyCachePlan {
    pub(crate) fn key(&self) -> Option<&rustfs_object_data_cache::ObjectDataCacheKey> {
        match self {
            Self::Cacheable(plan) => plan.key(),
            Self::Disabled | Self::Skip => None,
        }
    }
}

/// Builds a conservative body-cache plan for a GET request.
pub(crate) fn build_get_object_body_cache_plan(
    adapter: &ObjectDataCacheAdapter,
    request: GetObjectBodyCacheRequest<'_>,
) -> GetObjectBodyCachePlan {
    build_get_object_body_cache_plan_inner(adapter, request, true)
}

pub(crate) fn build_get_object_body_cache_plan_for_revalidation(
    adapter: &ObjectDataCacheAdapter,
    request: GetObjectBodyCacheRequest<'_>,
) -> GetObjectBodyCachePlan {
    build_get_object_body_cache_plan_inner(adapter, request, false)
}

fn build_get_object_body_cache_plan_inner(
    adapter: &ObjectDataCacheAdapter,
    request: GetObjectBodyCacheRequest<'_>,
    record_metric: bool,
) -> GetObjectBodyCachePlan {
    if adapter.is_disabled() {
        return GetObjectBodyCachePlan::Disabled;
    }

    if request.has_range
        || request.part_number.is_some()
        || request.encryption_applied
        || request.info.is_encrypted()
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
    // ODC-06 (backlog#1111): make the key write-unique. `data_dir` is the xl.meta
    // directory UUID that ecstore regenerates on every body write, so it is
    // distinct across overwrites regardless of content (MD5 collision) or
    // timestamp; it is the primary anchor. `mod_time` is a second anchor for the
    // rare read where `data_dir` is unresolved. Both derive here — the one place
    // the ecstore hook and the usecase layer share — so both sites produce an
    // identical key by construction.
    let data_dir_u128 = request.info.data_dir.map(|data_dir| data_dir.as_u128());
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
        data_dir_u128,
        mod_time_unix_nanos,
        body_variant: ObjectDataCacheBodyVariant::FullObjectPlainV1,
    };

    let engine_plan = if record_metric {
        adapter.plan_get(engine_request)
    } else {
        adapter.plan_get_untracked(engine_request)
    };
    match engine_plan {
        ObjectDataCacheGetPlan::Disabled | ObjectDataCacheGetPlan::SkipTooLarge => GetObjectBodyCachePlan::Skip,
        plan @ ObjectDataCacheGetPlan::Cacheable { .. } => GetObjectBodyCachePlan::Cacheable(plan),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GetObjectBodyCachePlan, GetObjectBodyCacheRequest, build_get_object_body_cache_plan,
        build_get_object_body_cache_plan_for_revalidation,
    };
    use crate::app::object_data_cache::ObjectDataCacheAdapter;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
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
    fn revalidation_plan_does_not_count_a_second_get() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let adapter = enabled_adapter();
            let info = crate::storage::storage_api::StorageObjectInfo {
                etag: Some("etag".to_string()),
                size: 4,
                actual_size: 4,
                ..Default::default()
            };
            let request = GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &info,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            };
            assert!(matches!(
                build_get_object_body_cache_plan(&adapter, request),
                GetObjectBodyCachePlan::Cacheable(_)
            ));
            assert!(matches!(
                build_get_object_body_cache_plan_for_revalidation(&adapter, request),
                GetObjectBodyCachePlan::Cacheable(_)
            ));
            assert!(matches!(
                build_get_object_body_cache_plan_for_revalidation(
                    &adapter,
                    GetObjectBodyCacheRequest {
                        response_content_length: 9 * 1024 * 1024,
                        ..request
                    }
                ),
                GetObjectBodyCachePlan::Skip
            ));
        });

        let plans = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _unit, _description, value)| {
                (composite.kind() == MetricKind::Counter && composite.key().name() == "rustfs_object_data_cache_plan_total")
                    .then_some(value)
            })
            .map(|value| match value {
                DebugValue::Counter(value) => value,
                _ => panic!("plan metric must be a counter"),
            })
            .sum::<u64>();
        assert_eq!(plans, 1);
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
    fn cold_fill_bypass_variants_never_join_session() {
        let adapter = enabled_adapter();
        let coordinator = adapter.cold_fill_coordinator();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            ..Default::default()
        };

        for (has_range, part_number, encryption_applied) in [(true, None, false), (false, Some(1), false), (false, None, true)] {
            let plan = build_get_object_body_cache_plan(
                &adapter,
                GetObjectBodyCacheRequest {
                    bucket: "bucket",
                    key: "object",
                    info: &info,
                    response_content_length: 4,
                    has_range,
                    part_number,
                    encryption_applied,
                },
            );
            assert!(matches!(plan, GetObjectBodyCachePlan::Skip));
            assert_eq!(coordinator.active_session_count_for_test(), 0);
        }

        let mut remote = info;
        remote.transitioned_object.status = "complete".to_string();
        let remote_plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &remote,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );
        assert!(matches!(remote_plan, GetObjectBodyCachePlan::Skip));
        assert_eq!(coordinator.active_session_count_for_test(), 0);

        let encrypted = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            user_defined: std::sync::Arc::new(std::collections::HashMap::from([(
                "x-amz-server-side-encryption".to_string(),
                "AES256".to_string(),
            )])),
            ..Default::default()
        };
        let encrypted_plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "object",
                info: &encrypted,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );
        assert!(matches!(encrypted_plan, GetObjectBodyCachePlan::Skip));
        assert_eq!(coordinator.active_session_count_for_test(), 0);

        let compressed = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            user_defined: std::sync::Arc::new(std::collections::HashMap::from([(
                rustfs_utils::http::SUFFIX_COMPRESSION.to_string(),
                "klauspost/compress/s2".to_string(),
            )])),
            ..Default::default()
        };
        let compressed_plan = build_get_object_body_cache_plan(
            &adapter,
            GetObjectBodyCacheRequest {
                bucket: "bucket",
                key: "compressed-object",
                info: &compressed,
                response_content_length: 4,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        );
        assert!(
            matches!(compressed_plan, GetObjectBodyCachePlan::Cacheable(_)),
            "a decoded full-object compressed read must remain cacheable"
        );
        assert_eq!(coordinator.active_session_count_for_test(), 0);
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
    fn planner_key_changes_with_data_dir() {
        // ODC-06 (backlog#1111): data_dir is regenerated on every body write, so
        // an overwrite yields a different key even when etag + size AND mod_time
        // are identical — the case mod_time alone cannot cover. Also confirms
        // data_dir actually reaches the key (data_dir_u128 == the UUID's u128).
        let adapter = enabled_adapter();
        let dir_a = uuid::Uuid::from_u128(0xA);
        let dir_b = uuid::Uuid::from_u128(0xB);
        let mut info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 4,
            actual_size: 4,
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp_nanos(1_000).unwrap()),
            data_dir: Some(dir_a),
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
        let key_a = cacheable_key(&make_request(&info));
        assert_eq!(key_a.data_dir_u128, Some(dir_a.as_u128()), "data_dir must reach the key");

        // Same etag/size/mod_time, only data_dir differs → different key.
        info.data_dir = Some(dir_b);
        let key_b = cacheable_key(&make_request(&info));
        assert_ne!(key_a, key_b, "keys differing only by data_dir must not collide");
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
