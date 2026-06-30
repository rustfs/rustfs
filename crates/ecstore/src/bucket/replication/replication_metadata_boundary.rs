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

use crate::bucket::metadata_sys;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use rustfs_utils::path::path_join_buf;
use s3s::dto::ReplicationConfiguration;
use time::OffsetDateTime;

pub(crate) const REPLICATION_DIR: &str = ".replication";
pub(crate) const RESYNC_FILE_NAME: &str = "resync.bin";
pub(crate) const MRF_REPLICATION_FILE: &str = "config/replication/mrf.bin";

pub(crate) async fn replication_config(bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
    metadata_sys::get_replication_config(bucket).await
}

pub(crate) async fn optional_replication_config(bucket: &str) -> Result<Option<ReplicationConfiguration>> {
    let config = match replication_config(bucket).await {
        Ok((config, _)) => Some(config),
        Err(err) => {
            if err != Error::ConfigNotFound {
                return Err(err);
            }
            None
        }
    };
    Ok(config)
}

pub(crate) fn rustfs_meta_bucket() -> &'static str {
    RUSTFS_META_BUCKET
}

pub(crate) fn resync_lock_key(bucket: &str, arn: &str) -> String {
    format!("{REPLICATION_DIR}/{bucket}/{arn}")
}

pub(crate) fn bucket_resync_dir_path(bucket: &str) -> String {
    path_join_buf(&[BUCKET_META_PREFIX, bucket, REPLICATION_DIR])
}

pub(crate) fn bucket_resync_file_path(bucket: &str) -> String {
    let resync_dir_path = bucket_resync_dir_path(bucket);
    path_join_buf(&[&resync_dir_path, RESYNC_FILE_NAME])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_metadata_paths_match_existing_layout() {
        assert_eq!(resync_lock_key("bucket-a", "arn-a"), ".replication/bucket-a/arn-a");
        assert_eq!(bucket_resync_dir_path("bucket-a"), "buckets/bucket-a/.replication");
        assert_eq!(bucket_resync_file_path("bucket-a"), "buckets/bucket-a/.replication/resync.bin");
        assert_eq!(MRF_REPLICATION_FILE, "config/replication/mrf.bin");
    }
}
