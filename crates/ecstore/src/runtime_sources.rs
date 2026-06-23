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

use std::sync::Arc;

use crate::{config::get_global_storage_class, global::get_global_deployment_id};
use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_kms::{ObjectEncryptionService, get_global_encryption_service};

pub(crate) fn record_erasure_write_quorum_failure(stage: &'static str, dominant_error: &'static str) {
    global_internode_metrics().record_erasure_write_quorum_failure(stage, dominant_error);
}

pub(crate) async fn object_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    get_global_encryption_service().await
}

pub(crate) fn storage_class_parity(storage_class: Option<&str>) -> Option<usize> {
    get_global_storage_class().and_then(|sc| sc.get_parity_for_sc(storage_class.unwrap_or_default()))
}

pub(crate) fn storage_class_should_inline(shard_size: i64, versioned: bool) -> bool {
    get_global_storage_class().is_some_and(|sc| sc.should_inline(shard_size, versioned))
}

pub(crate) fn deployment_upload_id(upload_id: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD
        .encode_to_string(format!("{}.{}", get_global_deployment_id().unwrap_or_default(), upload_id).as_bytes())
}

pub(crate) fn global_lock_manager() -> Arc<rustfs_lock::GlobalLockManager> {
    rustfs_lock::get_global_lock_manager()
}
