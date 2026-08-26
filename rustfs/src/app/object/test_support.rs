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

//! Test-only scaffolding shared by the object use-case test modules.

use super::*;
use http::{Extensions, HeaderMap, Method, Uri};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct MockUploadStreamSha256Mismatch;

impl std::fmt::Display for MockUploadStreamSha256Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UploadStreamError: Sha256Mismatch")
    }
}

impl std::error::Error for MockUploadStreamSha256Mismatch {}

pub(super) fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

pub(super) async fn real_cold_fill_test_context() -> (Arc<ECStore>, Arc<AppContext>) {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    if current_app_context().is_none() {
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
    }
    let ambient = current_app_context().expect("real cold-fill tests require an ambient AppContext");
    let context = temp_env::with_vars(
        [
            (rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, Some("true")),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MODE, Some("fill_materialize_enabled")),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_BYTES, Some("8388608")),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES, Some("2097152")),
            (rustfs_config::ENV_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT, Some("0")),
        ],
        || Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms())),
    );
    assert!(context.object_data_cache().materialize_fill_enabled());
    (store, context)
}

pub(super) async fn put_real_cold_fill_object(store: &Arc<ECStore>, bucket: &str, object: &str, body: &[u8]) -> ObjectInfo {
    let mut reader = PutObjReader::from_vec(body.to_vec());
    store
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("real cold-fill test object must be written")
}

pub(super) fn real_cold_fill_plan(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
) -> rustfs_object_data_cache::ObjectDataCacheGetPlan {
    let length = info
        .get_actual_size()
        .expect("real cold-fill test metadata must expose plaintext size");
    let GetObjectBodyCachePlan::Cacheable(plan) = build_get_object_body_cache_plan(
        adapter,
        GetObjectBodyCacheRequest {
            bucket,
            key: object,
            info,
            response_content_length: length,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        },
    ) else {
        panic!("real cold-fill test object must be cacheable");
    };
    plan
}
