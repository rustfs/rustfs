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

//! Migration of bucket metadata from legacy format to RustFS format.

use crate::bucket::metadata::BUCKET_METADATA_FILE;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::store_api::{BucketOptions, ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Legacy metadata bucket path, used when migrating bucket metadata.
pub(crate) const MIGRATING_META_BUCKET: &str = ".minio.sys";

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
