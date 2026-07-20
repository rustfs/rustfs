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

use super::*;
use rustfs_utils::http::headers::{AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestoreCleanupIdentity {
    version_id: Option<Uuid>,
    data_dir: Option<Uuid>,
    mod_time: Option<OffsetDateTime>,
    size: i64,
}

impl RestoreCleanupIdentity {
    fn from_object_info(obj_info: &ObjectInfo) -> Self {
        Self {
            version_id: obj_info.version_id,
            data_dir: obj_info.data_dir,
            mod_time: obj_info.mod_time,
            size: obj_info.size,
        }
    }

    fn matches_file_info(&self, fi: &FileInfo, expected_etag: &str) -> bool {
        self.version_id == fi.version_id
            && self.data_dir == fi.data_dir
            && self.mod_time == fi.mod_time
            && self.size == fi.size
            && expected_etag == get_raw_etag(&fi.metadata)
    }
}

impl SetDisks {
    pub async fn update_restore_metadata(
        &self,
        bucket: &str,
        object: &str,
        obj_info: &ObjectInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        if obj_info.bucket.is_empty() || obj_info.name.is_empty() {
            return Ok(());
        }
        let expected = RestoreCleanupIdentity::from_object_info(obj_info);
        let expected_operation_id = restore_operation_id_from_metadata(&opts.user_defined)?;
        let expected_etag = obj_info
            .etag
            .clone()
            .unwrap_or_else(|| get_raw_etag(obj_info.user_defined.as_ref()));
        let version_id = expected.version_id.map(|v| v.to_string());
        let lock_guard = if !opts.no_lock {
            Some(
                self.acquire_write_lock_diag("restore_cleanup_metadata", bucket, object)
                    .await?,
            )
        } else {
            None
        };
        let read_opts = ObjectOptions {
            version_id,
            versioned: opts.versioned,
            version_suspended: opts.version_suspended,
            ..Default::default()
        };
        let (mut fi, _, disks) = self
            .get_object_fileinfo_gated(bucket, object, &read_opts, false, false)
            .await?;
        if let Some(expected_operation_id) = expected_operation_id {
            match restore_operation_id_from_metadata(&fi.metadata)? {
                Some(actual_operation_id) if actual_operation_id == expected_operation_id => {}
                _ => return Ok(()),
            }
        }
        if !expected.matches_file_info(&fi, &expected_etag) {
            return Ok(());
        }
        fi.metadata.remove(X_AMZ_RESTORE.as_str());
        fi.metadata.remove(AMZ_RESTORE_EXPIRY_DAYS);
        fi.metadata.remove(AMZ_RESTORE_REQUEST_DATE);
        rustfs_utils::http::metadata_compat::remove_str(
            &mut fi.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
        );
        if lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(Error::other("restore cleanup lock lost before metadata update".to_string()));
        }
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        self.update_object_meta_with_opts(
            bucket,
            object,
            fi,
            disks.as_slice(),
            &UpdateMetadataOpts {
                replace_user_metadata: true,
                ..Default::default()
            },
        )
        .await?;
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(())
    }
}
