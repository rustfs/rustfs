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

//! Migration of bucket metadata from MinIO to RustFS format.

use crate::bucket::metadata::BUCKET_METADATA_FILE;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::store_api::{ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// metadata bucket path, used when migrating bucket metadata.
const MIGRATING_META_BUCKET: &str = ".minio.sys";

/// Migrates bucket metadata
/// using StorageAPI get_object_reader and put_object. Non-fatal: logs and returns on error.
/// Skips buckets that already exist in RustFS (idempotent).
pub async fn try_migrate_bucket_metadata<S: StorageAPI>(store: Arc<S>) {
    let prefix = format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}");
    let loi = match store
        .clone()
        .list_objects_v2(
            MIGRATING_META_BUCKET,
            &prefix,
            None,
            Some(SLASH_SEPARATOR.to_string()),
            1000,
            false,
            None,
            false,
        )
        .await
    {
        Ok(loi) => loi,
        Err(e) => {
            debug!("list migrating buckets failed (may not exist): {e}");
            return;
        }
    };

    let prefix = format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}");
    let buckets: Vec<String> = loi
        .prefixes
        .iter()
        .filter_map(|p| {
            let rest = p.trim_end_matches('/').strip_prefix(&prefix)?;
            let name = rest.trim_end_matches('/');
            if name.is_empty() || name.contains('/') {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect();

    if buckets.is_empty() {
        return;
    }

    info!("Migrating {} bucket metadata from MinIO config", buckets.len());

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
            warn!("write migrating bucket metadata {bucket}: {e}");
        } else {
            debug!("Migrated bucket metadata: {bucket}");
        }
    }
}
