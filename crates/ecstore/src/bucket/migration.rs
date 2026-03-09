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

//! Migration of bucket metadata and IAM config from legacy format to RustFS format.

use crate::bucket::metadata::BUCKET_METADATA_FILE;
use crate::disk::{BUCKET_META_PREFIX, MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
use crate::store_api::{BucketOptions, ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// IAM config prefix under meta bucket (e.g. config/iam/).
const IAM_CONFIG_PREFIX: &str = "config/iam";

/// Migrates bucket metadata from legacy format to RustFS.
/// Uses list_bucket (from disk volumes) to get bucket names, since list_objects_v2 on the legacy
/// meta bucket may not work (legacy format differs from object layer expectations).
/// Skips buckets that already exist in RustFS (idempotent).
pub async fn try_migrate_bucket_metadata<S: StorageAPI>(store: Arc<S>) {
    let buckets_list = match store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!("list buckets failed (skip migration): {e}");
            return;
        }
    };

    let buckets: Vec<String> = buckets_list.into_iter().map(|b| b.name).collect();

    if buckets.is_empty() {
        debug!("No migrating bucket metadata found");
        return;
    }

    debug!("Found {} migrating bucket metadata, migrating...", buckets.len());

    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let h = HeaderMap::new();

    for bucket in buckets {
        let meta_path = format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{bucket}{SLASH_SEPARATOR}{BUCKET_METADATA_FILE}");
        if store
            .get_object_info(RUSTFS_META_BUCKET, &meta_path, &ObjectOptions::default())
            .await
            .is_ok()
        {
            debug!("Bucket metadata already exists in RustFS, skip: {bucket}");
            continue;
        }
        let mut rd = match store
            .get_object_reader(MIGRATING_META_BUCKET, &meta_path, None, h.clone(), &opts)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!("read migrating bucket metadata {bucket}: {e}");
                continue;
            }
        };

        let data = match rd.read_all().await {
            Ok(d) if !d.is_empty() => d,
            Ok(_) => continue,
            Err(e) => {
                debug!("read migrating bucket metadata {bucket} body: {e}");
                continue;
            }
        };

        if let Err(e) = store
            .put_object(RUSTFS_META_BUCKET, &meta_path, &mut PutObjReader::from_vec(data), &opts)
            .await
        {
            warn!("write bucket metadata {bucket}: {e}");
        } else {
            info!("Migrated bucket metadata: {bucket}");
        }
    }
}

/// Migrates IAM config from legacy `.minio.sys/config/iam/` to `.rustfs.sys/config/iam/`.
/// Lists all objects under the IAM prefix in the source, copies each to the target if not present.
/// Skips objects that already exist in RustFS (idempotent).
/// If list_objects_v2 on the legacy bucket fails (e.g. format differs), migration is skipped.
pub async fn try_migrate_iam_config<S: StorageAPI>(store: Arc<S>) {
    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let h = HeaderMap::new();
    let prefix = format!("{IAM_CONFIG_PREFIX}/");
    let mut continuation: Option<String> = None;
    let mut total_migrated = 0usize;

    loop {
        let list_result = match store
            .clone()
            .list_objects_v2(MIGRATING_META_BUCKET, &prefix, continuation, None, 500, false, None, false)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!("list IAM config from legacy bucket failed (skip migration): {e}");
                return;
            }
        };

        for obj in list_result.objects {
            let path = &obj.name;
            if path.is_empty() || path.ends_with('/') {
                continue;
            }
            if store
                .get_object_info(RUSTFS_META_BUCKET, path, &ObjectOptions::default())
                .await
                .is_ok()
            {
                debug!("IAM config already exists in RustFS, skip: {path}");
                continue;
            }
            let mut rd = match store
                .get_object_reader(MIGRATING_META_BUCKET, path, None, h.clone(), &opts)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    debug!("read migrating IAM config {path}: {e}");
                    continue;
                }
            };
            let data = match rd.read_all().await {
                Ok(d) if !d.is_empty() => d,
                Ok(_) => continue,
                Err(e) => {
                    debug!("read migrating IAM config {path} body: {e}");
                    continue;
                }
            };
            if let Err(e) = store
                .put_object(RUSTFS_META_BUCKET, path, &mut PutObjReader::from_vec(data), &opts)
                .await
            {
                warn!("write IAM config {path}: {e}");
            } else {
                info!("Migrated IAM config: {path}");
                total_migrated += 1;
            }
        }

        continuation = list_result.next_continuation_token.or(list_result.continuation_token);
        if !list_result.is_truncated || continuation.is_none() {
            break;
        }
    }

    if total_migrated > 0 {
        info!("IAM migration complete: {} object(s) migrated", total_migrated);
    }
}
